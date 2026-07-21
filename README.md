这是一个ip扫描工具，主要作用是把你找到有效的ip:prot保存到config.ini中去，当失效时这工具的作用就来，它会帮你重新扫到有效的ip，当然了，除非整个ip挂了才会扫不到。3次扫不到就会禁止再扫，并把原保存下来的txt删除掉。
使用方法：
面板中选中要使用的节点，通过复制上方显示的对应分组的组播地址，就可以自动重定向到你选择的节点来播放。通过更换节点来切换其他ip而不用在播放地址中修改。
可以通过上方的日志查看扫描情况，也可以在配置编辑入口进去修改你的config.ini内容。
组播id文件夹中需要放置你收集到的整套id。也可以不放

自己部署来了解吧。有BUG的请反馈。

首次使用要进行token认证，只认证一次，除非token变化了才需要输入正确的token来再次认证。
默认Token是：
```
ilovechina
```

Compose部署
```
version: "3.8"

services:
  ipscan:
    image: ken01982/ipscan:latest
    container_name: ipscan
    restart: unless-stopped
    ports:
      - "6603:6603"
    environment:
      - CONFIG_PASSWORD=admin  #配置编辑页面默认的管理密码
      - ENABLE_SCANNER=1
      - SCAN_INTERVAL=900
    volumes:
      - ./data:/app/data
      - ./组播ID:/app/组播ID
```
