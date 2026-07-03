# QQ GTK/C Chat

这是一个面向 openEuler 课程设计的 C 语言 QQ 风格聊天系统示例：一个线程池 Socket 服务器，加一个 GTK4 + GDK + Cairo 图形客户端。

功能模块技术讲解见：[docs/module-technical-guide.md](docs/module-technical-guide.md)。

## 功能覆盖

- 图形化界面：GTK4 + CSS 深色 QQ 风登录窗口 + QQ 风格主界面，登录页包含圆形头像、账号/密码图标输入框、自动登录/记住密码、大号登录按钮、扫码/注册入口；主界面左侧“消息/好友/群聊/文件”导航可切换，右侧是带头像的左右气泡聊天区，Cairo 绘制头像。
- Socket 网络编程：客户端和服务器通过 TCP 长连接通信。
- 多线程/同步互斥：服务器启动固定工作线程池；客户端有 UI 主线程、服务器接收线程、P2P 文件监听线程、文件发送线程；共享数据使用 mutex/cond。
- 进程共享：服务器使用 POSIX shared memory `/qq_gtk_c_stats` 保存在线人数、登录次数、消息数，并用进程共享 mutex 保护。
- 私聊和群聊：私聊按目标用户路由；群聊按 `groups.tsv` 成员路由，不使用 UDP 广播；支持建群、群邀请、同意/拒绝入群、退群。
- 会话选择：客户端不需要手动切“私聊/群聊”，点击好友自动进入私聊，点击群自动进入群聊。
- 好友申请、好友数、在线好友数：添加好友先生成申请，接收方同意后才写入好友关系；客户端侧栏显示好友在线状态，好友登录或退出时会自动刷新相关好友的在线状态和在线好友数，支持删除好友。
- 离线消息：用户不在线时写入 `data/offline.tsv`，下次登录自动投递。
- 历史消息和消息检索：消息写入 `data/messages.tsv`，客户端可查看当前账号全部历史、自动加载当前会话历史，也可按关键字检索。
- 信息存储：用户账号/昵称/密码、好友、好友申请、群成员、群邀请、历史消息、离线消息都以 TSV 文件保存到 `data/`。
- 文件传输：服务器只提供在线用户地址查询，文件数据由客户端之间直接 TCP P2P 传输。
- 多线程文件传输：接收端常驻监听线程，发送端每个文件启动独立线程。
- 优雅退出：左侧“退出”按钮或关闭窗口都会发送 `QUIT`，关闭 socket 和 P2P 监听端口。
- 连接共享：一个客户端长连接同时承载私聊、群聊、历史、搜索、好友、P2P 地址查询等控制消息。

## openEuler 依赖

```bash
sudo dnf install -y gcc make pkgconf-pkg-config gtk4-devel
```

如果聊天输入框不能切换中文输入法，先安装并启动中文输入法框架，例如 fcitx5：

```bash
sudo dnf install -y fcitx5 fcitx5-gtk fcitx5-chinese-addons
export GTK_IM_MODULE=fcitx
export QT_IM_MODULE=fcitx
export XMODIFIERS=@im=fcitx
fcitx5 -d
```

然后重新启动 `./bin/qq_client`。程序内部已经按 UTF-8 发送和保存消息，中文消息本身是支持的。

## 编译

```bash
cd qq-gtk-c
make
```

会生成：

- `bin/qq_server`
- `bin/qq_client`

## 运行演示

先启动服务器：

```bash
./bin/qq_server 9090 8
```

再开两个终端启动两个客户端，启动后会弹出登录/注册窗口：

```bash
./bin/qq_client
./bin/qq_client
```

如果虚拟机终端出现 `libEGL`、`MESA-LOADER`、`zink` 之类提示，这是 Mesa/OpenGL 驱动警告，不是聊天功能错误。客户端默认设置了 Cairo/软件渲染来绕开这类问题。

演示建议：

1. 先注册 `alice` 和 `bob` 两个账号，再分别登录。
2. Alice 在“好友”页申请添加 `bob`，Bob 收到申请后点击“同意”。
3. 默认进入“消息”页，里面是历史会话；点击左侧“好友”才显示“我的好友”列表。
4. 在好友列表点击 `bob`，发送私聊。
5. 退出或重新登录 `bob`，Alice 侧好友列表中的在线/离线状态和在线好友数会自动变化。
6. 点击左侧“群聊”，可在输入框创建群；选中某个群后输入账号并点“邀请”，对方同意后加入。
7. 点击“历史”查看当前账号相关的全部历史记录（最多 1000 条）；点击某个好友或群会自动加载该会话历史。
8. 在“消息”页左侧检索框输入关键字，点击“检索”或按 Enter 查历史消息。
9. 私聊目标选择在线用户后，点击“文件”进行 P2P 文件传输；接收文件保存在工程目录 `downloads/`，同名文件会自动改名，文件页会显示收发记录。

## 数据文件

- `data/users.tsv`：用户账号、显示名、密码。
- `data/friends.tsv`：好友关系。
- `data/friend_requests.tsv`：尚未处理的好友申请。
- `data/groups.tsv`：群成员关系。
- `data/group_invites.tsv`：尚未处理的群邀请。
- `data/messages.tsv`：全部历史消息。
- `data/offline.tsv`：尚未投递的离线消息。

这些文件便于答辩时展示“用户信息存储、历史消息存储、离线消息存储”的实现。

## 协议简述

协议是按行分隔的文本协议，字段用 TAB 分隔，消息正文会做百分号转义。常见命令：

- `REGISTER user display peer_port password`
- `LOGIN user display peer_port password`
- `ADD_FRIEND user`
- `ACCEPT_FRIEND user`
- `REJECT_FRIEND user`
- `DELETE_FRIEND user`
- `MSGP to text`
- `CREATE_GROUP group`
- `INVITE_GROUP group user`
- `ACCEPT_GROUP group inviter`
- `REJECT_GROUP group inviter`
- `LEAVE_GROUP group`
- `MSGG group text`
- `HISTORY P|G scope limit`
- `HISTORY ALL * limit`
- `SEARCH keyword`
- `PEER_INFO user`
- `FILE_NOTICE to filename size`
- `QUIT`

服务端会主动推送 `FRIENDS_BEGIN`、`FRIEND user online`、`FRIENDS_END` 和 `COUNTS friends online_friends` 来刷新好友列表和在线好友数。好友登录或退出时，服务端会重新推送给受影响的在线好友，因此客户端不需要手动刷新状态。

这种协议简单、可抓包、可调试，适合课程设计讲解。

群聊不再内置公共 `lobby`。用户需要自己建群，或收到邀请后点击同意加入。
