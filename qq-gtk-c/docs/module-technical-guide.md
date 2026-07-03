# QQ GTK/C 功能模块技术讲解

本文档用于配合源码注释阅读项目，重点说明各功能模块的职责、关键函数、核心技术点和数据流。源码中的详细注释主要解释“这段代码怎么做”，本文档解释“为什么这样分模块、模块之间怎么协作”。

## 一、项目整体分层

项目分为三层：

| 层次 | 文件 | 职责 |
|---|---|---|
| 协议工具层 | `src/protocol.c`、`src/protocol.h` | 封装可靠发送、按行接收、百分号转义、文件名提取等客户端/服务端共用能力 |
| 服务端业务层 | `src/server.c` | 处理登录注册、好友、群聊、消息路由、历史、搜索、离线消息、P2P 地址查询和共享统计 |
| GTK 客户端层 | `src/client_gtk4.c` | 构建图形界面，维护客户端状态，发送控制命令，接收服务端消息，完成 P2P 文件传输 |

通信采用 C/S 架构。客户端登录后与服务端保持一条 TCP 长连接，所有聊天控制命令都复用这条连接。文件内容传输不经过服务端，而是在两个客户端之间额外建立 P2P TCP 连接。

## 二、通信协议模块

对应文件：`src/protocol.h`、`src/protocol.c`

核心函数：

| 函数 | 作用 |
|---|---|
| `qq_send_all` | 循环调用 `send()`，处理短写和 `EINTR`，确保指定字节全部发出 |
| `qq_send_line` | 发送一条协议行，并追加 `\n` 作为消息边界 |
| `qq_recv_line` | 从 socket 逐字节读取，直到遇到 `\n`，形成一条完整协议消息 |
| `qq_escape` | 将 `%`、TAB、换行、回车转义为 `%XX` |
| `qq_unescape` | 将 `%XX` 还原为原始字符 |
| `qq_basename_dup` | 从路径中提取文件名，避免发送完整本地路径 |
| `qq_trim_newline` | 去掉行尾 `\n`、`\r` |

技术要点：

1. TCP 是字节流，没有天然消息边界，所以项目用 `\n` 分隔每条协议消息。
2. 字段之间用 TAB 分隔，聊天正文和文件名可能包含 TAB 或换行，因此发送前必须转义。
3. `qq_send_all` 是网络发送的基础，避免一次 `send()` 只发送部分数据导致协议行残缺。

协议示例：

```text
LOGIN    alice    alice    45678    password
MSGP     bob      hello%09world
MSGG     group1   hello%0Agroup
```

实际网络传输中字段分隔符是 TAB，上面用空格只是为了阅读。

## 三、服务端并发模块

对应文件：`src/server.c`

核心结构：

| 结构 | 作用 |
|---|---|
| `Job` | accept 到的新连接任务，包含客户端 fd 和 IP |
| `JobQueue` | 线程安全环形队列，用于主线程和 worker 线程之间传递连接 |
| `Client` | 在线客户端状态，保存 fd、用户名、显示名、P2P 端口和发送锁 |
| `SharedStats` | POSIX 共享内存中的统计数据 |

核心函数：

| 函数 | 作用 |
|---|---|
| `main` | 创建监听 socket、启动 worker 线程池、循环 `accept()` |
| `queue_push` | 主线程把新连接放入任务队列 |
| `queue_pop` | worker 线程阻塞等待并取出任务 |
| `worker_main` | worker 主循环，取出连接后调用 `handle_client` |
| `handle_client` | 完成登录/注册，并持续处理该客户端后续命令 |

并发模型：

```text
main thread
  accept()
  queue_push()

worker thread 1..N
  queue_pop()
  handle_client()
```

技术要点：

1. 主线程只负责接收新连接，不执行复杂业务，避免慢客户端影响 accept。
2. worker 数量固定，避免每个连接创建一个新线程造成资源失控。
3. `JobQueue` 使用 mutex + condition variable 实现生产者消费者模型。
4. 每个在线 `Client` 有独立 `send_mutex`，防止多个线程同时给同一 socket 发送消息时串包。

## 四、服务端用户与关系模块

对应文件：`src/server.c`

相关数据文件：

| 文件 | 内容 |
|---|---|
| `data/users.tsv` | 用户账号、显示名、密码 |
| `data/friends.tsv` | 好友关系，双向保存 |
| `data/friend_requests.tsv` | 尚未处理的好友申请 |
| `data/groups.tsv` | 群成员关系 |
| `data/group_invites.tsv` | 尚未处理的群邀请 |

核心函数：

| 函数 | 作用 |
|---|---|
| `load_user_locked` | 从 `users.tsv` 读取用户资料 |
| `create_user_locked` | 注册用户并写入 `users.tsv` |
| `request_friend` | 发送好友申请 |
| `accept_friend` | 同意好友申请，写入双向好友关系 |
| `reject_friend` | 拒绝好友申请 |
| `delete_friend` | 删除好友关系 |
| `send_friend_list` | 发送好友列表和每个好友的在线状态 |
| `send_counts` | 发送好友总数和在线好友数 |
| `refresh_friends_of_user` | 某个用户登录或退出后刷新相关好友的侧栏状态 |
| `create_group` | 创建群并把创建者加入群 |
| `invite_group` | 邀请用户加入群 |
| `accept_group` | 同意群邀请 |
| `leave_group` | 退出群 |

技术要点：

1. 好友不是单方面添加，必须先写入申请，接收方同意后才成为好友。
2. 好友关系在 `friends.tsv` 中双向保存，查询某个用户好友时只看第一列等于该用户的记录。
3. 好友在线状态由服务端在线表统一计算。用户登录或退出后，服务端重新推送受影响好友的 `FRIENDS` 和 `COUNTS` 数据。
4. 群聊不是公共广播，而是按 `groups.tsv` 中的成员列表逐个定向转发。
5. 所有 TSV 文件读写都受 `g_store_mutex` 保护，避免多个 worker 同时写文件造成数据交叉。

## 五、服务端消息模块

对应文件：`src/server.c`

相关数据文件：

| 文件 | 内容 |
|---|---|
| `data/messages.tsv` | 所有历史消息 |
| `data/offline.tsv` | 尚未投递的离线消息 |

核心函数：

| 函数 | 作用 |
|---|---|
| `route_private` | 私聊消息路由 |
| `route_group` | 群聊消息路由 |
| `store_message` | 将消息追加到 `messages.tsv` |
| `store_offline` | 将离线消息写入 `offline.tsv` |
| `deliver_offline` | 用户登录后投递并删除离线消息 |
| `send_history` | 按私聊、群聊或全部历史返回消息 |
| `send_search` | 根据关键字检索当前用户有权限看到的历史消息 |

私聊流程：

```text
客户端 A 发送 MSGP
  服务端 store_message 写历史
  如果 B 在线：直接 send_client 到 B
  如果 B 离线：store_offline 写离线消息
```

群聊流程：

```text
客户端 A 发送 MSGG
  服务端检查 A 是否是群成员
  store_message 写历史
  读取 groups.tsv 得到群成员
  对每个在线成员直接投递
  对离线成员写 offline.tsv
```

技术要点：

1. 历史消息和离线消息是两个不同概念：历史永久保存，离线消息投递后删除。
2. `send_history` 和 `send_search` 会做权限过滤，用户只能看到自己参与的私聊或所在群的群聊。
3. 正文存储前使用百分号转义，保证 TSV 文件结构不会被用户输入破坏。

## 六、服务端共享内存统计模块

对应文件：`src/server.c`

核心函数：

| 函数 | 作用 |
|---|---|
| `init_shared_stats` | 创建或打开 POSIX 共享内存 `/qq_gtk_c_stats` |
| `stats_login` | 更新登录统计 |
| `stats_online` | 更新在线人数 |
| `stats_message` | 更新私聊/群聊消息计数 |

技术要点：

1. 使用 `shm_open` 创建共享内存对象。
2. 使用 `ftruncate` 设置共享内存大小。
3. 使用 `mmap` 映射到进程地址空间。
4. 使用 `pthread_mutexattr_setpshared` 创建进程共享 mutex。

这部分主要用于展示“进程间共享”的课程要求。即使只有一个服务端进程，也能说明如何把统计信息放到可被其他进程映射的共享区域中。

## 七、GTK 客户端 UI 模块

对应文件：`src/client_gtk4.c`

核心结构：

| 结构 | 作用 |
|---|---|
| `AppState` | 客户端全局状态，保存 GTK 控件、socket、线程、当前会话和本地缓存 |
| `FriendInfo` | 好友列表项 |
| `GroupInfo` | 群列表项 |
| `FileInfo` | 文件收发记录 |
| `RequestInfo` | 好友申请或群邀请 |
| `UiMessage`、`UiFriend` 等 | 后台线程投递给 GTK 主线程的 UI 更新任务 |

核心函数：

| 函数 | 作用 |
|---|---|
| `build_login` | 构建登录/注册窗口 |
| `build_main` | 构建主聊天界面 |
| `apply_css` | 注册 GTK CSS 样式 |
| `render_sidebar` | 根据当前页面渲染左侧列表 |
| `choose_conversation` | 切换当前好友/群会话并加载历史 |
| `send_clicked` | 发送私聊或群聊消息 |
| `receiver_main` | 后台读取服务端消息并分发 |

技术要点：

1. GTK 控件只能在主线程更新，所以接收线程必须通过 `g_idle_add` 投递 UI 任务。
2. 左侧栏不是固定内容，而是根据 `active_tab` 和本地缓存动态重绘。
3. `current_mode` 区分私聊和群聊，用户点击好友或群后自动设置，无需手动切换发送模式。
4. `AppState` 集中保存控件指针，方便多个事件回调访问同一份状态。

## 八、客户端接收线程模块

对应文件：`src/client_gtk4.c`

核心函数：`receiver_main`

服务端命令分发：

| 命令 | 客户端处理 |
|---|---|
| `OK` / `ERR` | 显示操作结果或错误 |
| `FRIENDS_BEGIN` / `FRIEND` | 刷新好友缓存和侧栏 |
| `GROUPS_BEGIN` / `GROUP` | 刷新群缓存和侧栏 |
| `COUNTS` | 更新好友数和在线好友数 |
| `FRIEND_REQUEST` | 添加好友申请通知 |
| `GROUP_INVITE` | 添加群邀请通知 |
| `MSG` | 显示实时聊天气泡 |
| `OFFLINE` | 显示离线消息 |
| `HISTORY` | 显示历史消息 |
| `SEARCH` | 显示检索结果 |
| `PEER` | 唤醒文件发送线程 |
| `BYE` | 结束接收线程 |

技术要点：

1. `receiver_main` 是唯一持续读取服务端控制连接的线程，避免多个线程同时 `recv` 导致协议行错乱。
2. 文件发送线程需要 P2P 地址时，只发送 `PEER_INFO`，真正读取 `PEER` 响应仍由 `receiver_main` 统一完成。
3. 好友在线状态变化也走同一条控制连接。服务端重新推送 `FRIENDS_BEGIN`、`FRIEND`、`COUNTS` 后，客户端清空并重建本地好友缓存。
4. 收到 `PEER` 后，客户端通过 `peer_cond` 唤醒正在等待的 `file_send_main`。

## 九、P2P 文件传输模块

对应文件：`src/client_gtk4.c`

核心函数：

| 函数 | 作用 |
|---|---|
| `setup_file_listener` | 绑定随机 P2P 文件接收端口 |
| `file_listener_main` | 常驻接收文件线程 |
| `wait_peer` | 查询并等待目标用户 P2P 地址 |
| `file_send_main` | 文件发送线程 |
| `make_download_path` | 生成不覆盖旧文件的保存路径 |

发送流程：

```text
用户选择文件
  file_send_main 线程启动
  发送 PEER_INFO 到服务端
  wait_peer 等待 PEER 响应
  connect 对方 P2P 端口
  发送 FILE_NOTICE 给服务端作为聊天通知
  通过 P2P socket 发送 QQFILE 头和文件数据
```

接收流程：

```text
file_listener_main accept 连接
  读取 QQFILE 头
  解析发送者、文件名、文件大小
  保存到 downloads/
  添加文件记录并显示通知
```

技术要点：

1. 服务端不转发文件内容，只提供在线用户 IP 和 P2P 端口。
2. 文件数据使用新的 TCP 连接传输，不占用聊天控制长连接。
3. 每个文件发送任务独立线程执行，避免大文件阻塞 GTK 主线程。
4. 接收端对文件名使用 `qq_basename_dup`，避免对方传入路径导致保存到非预期目录。

## 十、持久化数据模块

对应目录：`data/`

文件格式统一使用 TSV：

```text
字段1<TAB>字段2<TAB>字段3\n
```

设计原因：

1. 方便课程答辩时直接打开文件展示数据变化。
2. 与网络协议同样使用 TAB 分隔和百分号转义，代码复用简单。
3. 不依赖数据库服务，部署和演示成本低。

注意点：

1. TSV 适合课程设计和小规模演示，不适合高并发大数据量生产系统。
2. 项目用 `g_store_mutex` 保护文件读写，但没有数据库事务能力。
3. 后续扩展可替换为 SQLite/MySQL，并保留上层业务逻辑。

## 十一、启动和退出流程

服务端启动：

```text
main
  ensure_storage
  init_shared_stats
  socket/bind/listen
  创建 worker 线程
  accept 循环
```

客户端启动：

```text
main
  初始化 AppState
  初始化下载目录和 P2P 监听端口
  创建 GTKApplication
  build_login
  登录成功后 build_main + receiver_main
```

优雅退出：

1. 客户端发送 `QUIT`。
2. 客户端关闭控制 socket 和 P2P 监听 socket。
3. 服务端从在线表移除用户并刷新在线统计。
4. 服务端调用 `refresh_friends_of_user`，把最新好友在线状态推送给相关在线好友。
5. 客户端等待接收线程和文件监听线程结束。

## 十二、答辩讲解建议

可以按下面顺序讲：

1. 先讲整体 C/S 架构和一条 TCP 长连接复用多个控制命令。
2. 再讲服务端线程池：主线程 accept，worker 线程处理客户端。
3. 讲协议层：按行分帧、TAB 分字段、百分号转义。
4. 讲消息路由：私聊定向转发，群聊按成员列表转发。
5. 讲持久化：用户、好友、群、历史、离线消息都落到 TSV。
6. 讲客户端 GTK：主线程更新 UI，后台线程接收消息，用 `g_idle_add` 切回主线程。
7. 讲 P2P 文件：服务端只查地址，文件内容客户端直连。
8. 最后讲共享内存统计和项目局限。

项目局限可以主动说明：

1. 密码明文保存，仅适合课程演示，实际系统应加盐哈希。
2. 通信未加 TLS，实际系统应加密传输。
3. TSV 文件没有数据库事务能力，高并发场景应替换数据库。
4. P2P 文件传输适合同一局域网或可直连场景，复杂 NAT 环境需要中继或打洞。
