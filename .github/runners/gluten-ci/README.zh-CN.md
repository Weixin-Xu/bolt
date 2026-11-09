<!--
Copyright (c) ByteDance Ltd. and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Bolt 外部 CI 运维手册

ECS 上的 listener 轮询公开 Gluten PR，runner 通过本机 Docker 执行构建与测试，commenter 更新 PR 评论。Gluten 仓库维护可移植测试入口和矩阵。首次上线只发布评论，合入权限由目标仓库管理。[Gluten 测试入口](https://github.com/apache/gluten/tree/main/dev/bolt-ci)

## 准备 ECS

使用 Debian 12 x86_64、已更新的 Python 3.11、Git、Docker Engine、Docker Compose v2 插件、`iproute2`、`iptables` 和 systemd。使用本机的 rootful Docker 及其 iptables 防火墙后端。IPv4 转发和 Linux bridge netfilter 必须启用。宿主机和 worker 需要公网 DNS 及 GitHub、Adoptium、Debian、Maven Central、PyPI、Spark 下载站的访问能力。状态和作业目录需要持久的本地文件系统。按可用资源调整并发、CPU、内存和磁盘预检下限。

在 root shell 执行以下操作。示例中的 `/data` 替换为实际的 root 所有数据根目录。使用发布方提供的外部 SHA-256 验证归档，再解包到固定、版本化的 root 管理目录。升级时为新版本建立新目录，保留旧目录用于回滚。

```bash
install -d -m 0755 /data/gluten-external-ci/deploy/release
cd /data/gluten-external-ci/deploy/release
(cd /path/to && sha256sum -c bolt-gluten-ci.tar.gz.sha256) || exit 1
tar --no-same-owner -xzf /path/to/bolt-gluten-ci.tar.gz
cd bolt-gluten-ci/deploy
python3 -B bundle.py verify ..
install -m 0600 env.example /etc/bolt-gluten-ci.env
install -d -m 0700 /etc/bolt-gluten-ci-secrets
```

通过本地保密方式分别建立 `/etc/bolt-gluten-ci-secrets/reader` 和 `writer`，root 所有，权限 `0600`，各文件只有一个 PAT。读取凭据只需读取公开仓库 API，写凭据所属账号须能在目标 PR 发评论。GitHub 对第三方公开仓库的 fine-grained PAT 有能力限制，按账号与组织政策选择适用凭据。准备阶段只读探测 GitHub，写权限由指定 PR 的真实评论验证。[GitHub PAT 文档](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)

编辑 `/etc/bolt-gluten-ci.env`，将 `ONLY_PR` 改为已选定的真实测试 PR，`BOLT_REF` 改为预期的 Bolt 引擎完整 40 位 SHA。受信 driver 固定在 bundle 内。`BOLT_CHECKOUT_SEED` 可以指向持有该提交的独立可信 Bolt checkout，用 Git 传输复制对象。省略时从 Bolt 公共仓库获取指定 SHA。CI 现有镜像标签保留，准备阶段检查本地镜像并在需要时拉取，再解析为不可变 digest。

沿用旧 Compose project `runners` 的 Conan 服务时，用下面的标签查询填写 `CONAN_CACHE_CONTAINER`。预期只出现一个运行中容器。

```bash
docker ps --no-trunc --filter label=com.docker.compose.project=runners \
  --filter label=com.docker.compose.service=conan_server --format '{{.ID}}'
```

模板指向已发布的 Conan TCP 9300。使用 `host.docker.internal` 时，预检核对实际 host gateway、绑定端口和该 Conan 容器的目标地址。若旧容器重建，填写新 ID，停下外部 CI 后重新执行准备。也可以配置单个数字 IPv4 和端口，或把 `CONAN_REMOTE`、`CONAN_CACHE_CONTAINER` 都留空。保留已有 Arrow 缓存路径。配置仅接受无需凭据的 Conan URL。

## 单 PR 验证与切换

```bash
source /etc/bolt-gluten-ci.env
./ci.sh prepare
./ci.sh enqueue --pr "$ONLY_PR" --dry-run
./ci.sh up
./ci.sh enqueue --pr "$ONLY_PR"
./ci.sh status
./ci.sh logs
```

`prepare` 可重复运行，会保留原有 registry、镜像标签、Conan 服务、Arrow 源文件和缓存。它校验 bundle、准备固定引擎 checkout、为 Linux/amd64 下载并验证 Temurin JDK 8、11、17、21 和 25。辅助工具从同一 worker 镜像的 Debian 包仓库提取到单独的只读前缀。准备阶段会实际以 UID 1000 运行工具与网络探针。新 Compose project 固定为 `bolt-gluten-ci`。独立控制网络和专用 worker bridge 分离，worker 容器无凭据、控制状态或 Docker socket。配置只保存凭据文件路径。

预览读取 PR 和变更，不入队、不执行 PR 代码、不发评论。首启时 listener 将已有打开的 PR 建立基线，所以此处需显式入队。查看同一 PR 的当前 SHA、排队、开始和结果评论。核对 native 测试成功后能派发 14 个打包项和 27 个 UT 项，真实失败会写入结果。查看私有日志与报告确认 JDK、下载、缓存路径和失败分类。测试可以失败，失败本身也是应验证的回写路径。

正式切换时先等待旧 Actions 作业退出，并在 fork 的 Actions 页面停用仍部署在默认分支上的 `Bolt CI Listener`、`Bolt CI Commenter`、`Bolt CI Control Tests` 和 `Bolt Backend Build (x86)`。确认无重复入队后，使用标签只停止旧的 self-hosted runner 容器。请先检查下列两条查询的输出。

```bash
for service in runner-medium runner-small; do
  docker ps --filter label=com.docker.compose.project=runners \
    --filter "label=com.docker.compose.service=$service" \
    --format '{{.ID}} {{.Names}} {{.Status}}'
done

for service in runner-medium runner-small; do
  docker ps --quiet --filter label=com.docker.compose.project=runners \
    --filter "label=com.docker.compose.service=$service" |
    xargs -r docker stop
done
```

验证期间如果两套执行器仍在使用同一 ECS，先排空旧任务并预留资源，再执行真实入队。上述命令保留 registry、Conan 和源缓存。停止外部服务，清空环境文件中的 `ONLY_PR` 后重新 source、prepare、up，即可开启自动监听。

```bash
./ci.sh stop
# 编辑 /etc/bolt-gluten-ci.env，将 ONLY_PR 设为空值。
source /etc/bolt-gluten-ci.env
./ci.sh prepare
./ci.sh up
./ci.sh status
```

PR 作者或配置的维护者可在 PR 留一条完整的 `Run Gluten Bolt CI` 评论重跑，也支持 `Run Gluten Bolt CI on x86`。等待 30 分钟且始终未领取时，评论显示本次未执行。Apache 分支保护和原 fork 的 required check 由各仓库单独管理。旧的 Actions gate 与本服务评论使用独立机制。

## 数据、重启和排障

- `./ci.sh status` 显示项目容器和队列统计，`./ci.sh logs` 显示控制日志。原始 worker 日志、报告和产物位于 `${HOST_DATA_DIR}/gluten-external-ci/jobs`，仅供宿主机私下查看。
- `./ci.sh stop` 先停止 listener 和 runner 以冻结调度，再停止本实例的活跃 worker 并记录任务中断，最后停止 commenter。持久数据、缓存、worker bridge、防火墙和续期 timer 保留。恢复服务运行 `./ci.sh up`。更换镜像、PAT 文件、缓存地址或部署版本时，先 stop，再修改并重新 prepare、up。用持久旧 bundle 回滚时也执行相同步骤。
- `${HOST_DATA_DIR}/gluten-external-ci/control/state` 存放 SQLite WAL 数据。`assets` 存放版本化 JDK、工具和引擎。`cache/maven` 只接收经控制器独立校验的外部发行版 JAR/POM。原 Maven 缓存保留在原处，逐个核验需要迁移的公开发行版文件。排除 `settings.xml`、凭据和本地 SNAPSHOT。用可信快照选填 `cache/seeds/{conan,spark,pip}`，Spark 资源按 driver 期望的版本键和完成标记组织。runner 将 seed 复制到作业私有缓存。大体量的既有 ccache 建议使用小型可信快照并测量复制开销。
- 从旧版控制器升级时，停止服务后先备份 SQLite。旧记录没有目标分支名，升级会保留历史，停用这些记录中仍在排队或运行的请求及待发评论。Listener 重新核实受影响的开放 PR 的目标分支和完整 diff 后再按规则入队，未入队的旧基线 PR 不会仅因升级而触发。回滚到不识别目标分支字段的旧版时，需使用升级前的数据库备份。
- systemd 的 `bolt-gluten-ci-network.timer` 每约 20 秒复核实际防火墙、网桥身份和已配置的 Conan 目标。规则丢失、桥重建、缓存身份变化或证明过期均阻止 worker 继续执行。检查 `systemctl status bolt-gluten-ci-network.timer` 和 `journalctl -u bolt-gluten-ci-network.service`，排除原因后 stop、prepare、up。Docker 原生 nftables 后端缺少所需的 iptables hook，预检会失败。
- 重启 ECS 后先确认 guard 恢复续期，核对 listener/runner/commenter 的持久状态。断电期间没有评论或新任务；恢复后由同一数据库继续观察。Docker worker 的有效性由控制器重查当前 PR 和记录的容器身份。
- 遇到故障优先保留私有报告和数据库，禁止使用 `docker compose down -v` 清理这两套项目。检查旧服务时使用显式项目和 Compose 文件，或使用上面的标签查询，避免继承旧环境的 `COMPOSE_FILE`。

防火墙路径和 JDK/辅助工具由 ECS 上的 `prepare` 验证。验证日志、两种元数据地址与控制探针的隔离、一个真实 PR 的全矩阵运行及实际评论是现场验收项。[Docker 防火墙文档](https://docs.docker.com/engine/network/packet-filtering-firewalls/) [Adoptium CI 下载说明](https://adoptium.net/installation/ci-scripts)

## 从已提交版本生成独立 bundle

准备包含固定 driver commit 及 `.github/scripts` 对象的本地 Bolt checkout。使用两个已审核的完整 commit SHA，打包器从 Git 对象读取部署文件和 Gluten 帮助脚本。

```bash
python3 -B bundle.py create \
  --bolt-repo /path/to/bolt --bolt-revision "$BOLT_DEPLOY_SHA" \
  --gluten-repo /path/to/gluten --gluten-revision "$GLUTEN_HELPER_SHA" \
  --driver-repo /path/to/pinned-bolt-driver \
  --output /path/to/bolt-gluten-ci.tar.gz
cd /path/to
sha256sum bolt-gluten-ci.tar.gz > bolt-gluten-ci.tar.gz.sha256
```

归档中的清单记录两个仓库和 driver 的版本、每个文件的 SHA-256 与执行位。归档可在 helper 合入上游前部署。部署包和摘要通过可信发布渠道交付。
