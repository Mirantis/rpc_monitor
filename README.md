# RPC Monitoring tools for oslo.messaging



# Installation guide:

1) Apply patch for oslo.messaging (only mos 9.0 avaliable now):
  *  on fuel master node: `git clone https://github.com/kbespalov/rpc_monitor && cd ./rpc_monitor/setup/`
  *  execute `python ./patch_mos.py`
  *  if you need to change patching settings `vim patch_mos.py`
  *  ```py
        # ----- SETTINGS ---------
        # patch location
        PATCH = './mos_patches/mos9.patch'
        # destination path for scp
        SCP_TO = '/usr/lib/python2.7/dist-packages/'
        # do revers patch
        REVERS = False
        # ----- SETTINGS ---------
      ```
  * patched nodes are written in `./mos_patch/patched_nodes.txt`

2) Restart all openstack services
 * `python ./restart_services.py`

3) Choose any compute node as monitoring service backend:
 - `ssh node-x`
 - setup influxdb:
 ``` sh
 wget https://dl.influxdata.com/influxdb/releases/influxdb_0.13.0_amd64.deb
 sudo dpkg -i influxdb_0.13.0_amd64.deb
 sudo service influxdb start
 ```
 - setup grafana:
  ``` sh
  wget https://grafanarel.s3.amazonaws.com/builds/grafana_3.1.0-1468321182_amd64.deb
  sudo dpkg -i grafana_3.1.0-1468321182_amd64.deb
  sudo service grafana-server start
  ```
  - choose any controller node with enabled rabbitmq managment panel and make ssh tunnel
  ```
  # rabbitmq managment panel binds on localhost, so tunnel is needed
  ssh 192.168.0.17 -L 15674:localhost:15672
  ```
  - download monitor `git clone https://github.com/kbespalov/rpc_monitor && cd ./rpc_monitor`
  - `pip install -r requirements.txt`
  - Configure ip, port and rabbitmq credentials in `monitor.ini`
      ``` ini
      [DEFAULT]
      backend = rabbit
      workers = 16
      poll_delay = 30
      log_level = INFO
      listener_only = False

      [rabbit_monitor]
      # tunnel is used, so localhost
      management_url = http://localhost:15674
      # just copy nova credentials from /etc/nova/nova.conf
      management_user = nova
      management_pass = nova_password
      amqp_url = amqp://nova:nova_password@192.168.0.17:5673

      [influx_repository]
      db = rpc_monitor
      host = localhost
      port = 8086
      user = root
      password = root
      ```
  - run `python ./run.py --config-file monitor.ini`
  - if all is okay, samples from services are collected to influxdb via monitor

4) Configure Grafana (`admin:admin`):
    * Create new datasource `http://localhost:3000/datasources` (default must be True)
    * ![](http://i.imgur.com/fqhRkuz.png)
    * Import dashboard from file `http://localhost:3000/dashboard/new?editview=import`:
        * file located at: `rpc_monitor/setup/grafana/dashboard.json`
    * go to imported dashboard, you must see somethink like that:
    ![](http://i.imgur.com/NWSmAxt.png)