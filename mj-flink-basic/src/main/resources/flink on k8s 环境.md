



# Flink On Kubernetes 环境配置





## Deploying the operator 环境安装



Kubernetes 集群上安装证书管理器以启用添加 webhook 组件（每个 Kubernetes 集群只需要一次）



```bash
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
```

#### 使用 Helm 部署所选的稳定 Flink Kubernetes Operator 版本

##### 安装helm

下载对应版本，解压（这里已linux 为例）

```bash
tar -zxvf helm-v3.0.0-linux-amd64.tar.gz
```

在解压目录中找到`helm`程序，移动到需要的目录中(

```bash
mv linux-amd64/helm /usr/local/bin/helm
```

具体使用

```bash
helm help
```

##### 使用helm添加flink-kubernetes-operator包仓库

```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-<OPERATOR-VERSION>/
```

##### 安装flink-kubernetes-operator

这里可以指定命名空间来运行flink-kubernetes-operator pod

```bash
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator -n <namespaces>
```

##### 查询pod 是否运行

```bash
kubectl get pods -n <namespaces>
```

查询结果

```bash
NAME READY STATUS RESTARTS AGE
flink-kubernetes-operator-fb5d46f94-ghd8b 2/2 Running 0 4m21s
```

##### 使用helm 查询

```bash
helm list
```

查询结果

```bash
NAME NAMESPACE REVISION UPDATED STATUS CHART APP VERSION
flink-kubernetes-operator default 1 2022-03-09 17 (tel:12022030917):39:55.461359 +0100 CET deployed flink-kubernetes-operator-1.11-SNAPSHOT 1.11-SNAPSHOT
```

##### 提交作业测试

采用Kubernetes Deployment Commit Mode提交

```bash
kubectl create -f application-deployment.yaml -n flink 
```

查看运行任务

```bash
kubectl get pods -n flink
```

运行结果

```bash
NAME                                        READY   STATUS    RESTARTS   AGE
application-deployment-667869d6f5-pd9kb     1/1     Running   0          28m
application-deployment-taskmanager-1-1      1/1     Running   0          28m
application-deployment-taskmanager-1-2      1/1     Running   0          28m
flink-kubernetes-operator-864556787-qjzgq   2/2     Running   0          133m
```



## 构建镜像DockerFile

基于docker hub 下载对应版本

```dockerfile
# 使用本地镜像作为基础镜像
FROM flink:1.19.1-scala_2.12-java17

RUN mkdir -p $FLINK_HOME/usrlib

COPY config.properties /opt/flink/usrlib/
COPY FlinkDemo-1.0-SNAPSHOT.jar /opt/flink/usrlib/
COPY config.yaml /opt/flink/conf/
COPY lib /opt/flink/lib/
COPY commons-io-2.17.0.jar /opt/flink/lib/
COPY flink-s3-fs-hadoop-1.19.1.jar /opt/flink/plugins/s3-fs-hadoop/
```



## Kubernetes环境事项

### 使用命令创建NameSpace

```bash
kubectl create namespace flink
```

### 创建serviceaccount

```bash
kubectl create serviceaccount flink-service-account
```

### 用户授权

```bash
kubectl create clusterrolebinding flink-role-binding-flink --clusterrole=edit --serviceaccount=flink:flink-service-account
```

### Flink On Kubernetes Service 模版

`jobmanager-service.yaml` 。可选的 service，仅在非 HA 模式下使用

```yaml
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager
spec:
  type: ClusterIP
  ports:
  - name: rpc
    port: 6123
  - name: blob-server
    port: 6124
  - name: webui
    port: 8081
  selector:
    app: flink
    component: jobmanager
```

`jobmanager-rest-service.yaml`。可选的 service，该 service 将 jobmanager 的 `rest` 端口暴露为公共 Kubernetes node 的节点端口

```yaml
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager-rest
spec:
  type: NodePort
  ports:
  - name: rest
    port: 8081
    targetPort: 8081
    nodePort: 30081
  selector:
    app: flink
    component: jobmanager
```

`taskmanager-query-state-service.yaml` 可选服务，公开 TaskManager 端口以访问可查询状态作为公共 Kubernetes 节点的端口。

```yaml
apiVersion: v1
kind: Service
metadata:
  name: flink-taskmanager-query-state
spec:
  type: NodePort
  ports:
  - name: query-state
    port: 6125
    targetPort: 6125
    nodePort: 30025
  selector:
    app: flink
    component: taskmanager
```



## Flink On Kubernetes Commit Job Mode

#### Flink Run Commit Mode

```bash
./bin/flink run-application \
    --target kubernetes-application \
    -c org.example.Main \
    -Dkubernetes.namespace=flink \
    -Dkubernetes.jobmanager.service-account=flink-service-account \
    -Dkubernetes.cluster-id=application-cluster-demo \
    -Dkubernetes.container.image=flink-demo:1.19.1 \
    -Dkubernetes.rest-service.exposed.type=NodePort \
    local:///opt/flink/usrlib/FlinkDemo-1.0-SNAPSHOT.jar /opt/flink/usrlib/config.properties
```

#### Kubernetes FlinkDeployment Commit Mode

##### 编写FlinkDeployment 文件

Flink-application-deployment.yaml

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: flink
  name: application-deployment
spec:
  image: flink-demo:1.19.1
  flinkVersion: v1_19
  imagePullPolicy: IfNotPresent
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    env.java.opts: "-Duser.timezone=Asia/Shanghai"
    state.checkpoints.dir: "s3://flink-ck/etl/"  # 配置检查点存储路径
    state.savepoints.dir: "s3://flink-savepoints/etl/"    # 可选：配置保存点存储路径
  serviceAccount: flink-service-account
  jobManager:
    replicas: 1
    resource:
      memory: "2g"
      cpu: 1
  taskManager:
    replicas: 3
    resource:
      memory: "6g"
      cpu: 2
  job:
    jarURI: local:///opt/flink/usrlib/FlinkDemo-1.0-SNAPSHOT.jar
    entryClass: org.example.Main
    args:
      - "/opt/flink/usrlib/config.properties"
    parallelism: 6
    upgradeMode: savepoint
```



带flink配置

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  namespace: flink
  name: flink-application-deployment
spec:
  image: flink-demo:1.19.1
  flinkVersion: v1_19
  imagePullPolicy: IfNotPresent
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    env.java.opts: "--add-exports=java.base/sun.net.util=ALL-UNNAMED --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED -Duser.timezone=Asia/Shanghai"
    execution.checkpointing.interval: "5min"
    execution.checkpointing.externalized-checkpoint-retention: "RETAIN_ON_CANCELLATION"
    execution.checkpointing.max-concurrent-checkpoints: "6"
    execution.checkpointing.min-pause: "500ms"
    execution.checkpointing.mode: "AT_LEAST_ONCE"
    execution.checkpointing.timeout: "60min"
    execution.checkpointing.tolerable-failed-checkpoints: "3"
    execution.checkpointing.unaligned: "true"
    state.backend.type: "rocksdb"
    state.backend.incremental: "true"
    state.checkpoints.dir: "s3://flink-cks/etl/"
    state.savepoints.dir: "s3://flink-sps/etl/"
    s3.endpoint: "http://172.16.141.225:19000"
    s3.path-style-access: "true"
    s3.access-key: "admin"
    s3.secret-key: "Nspt@1234"
    rest.flamegraph.enabled: "true"
    pekko.ask.timeout: "180s"
  serviceAccount: flink-service-account
  jobManager:
    replicas: 1
    resource:
      memory: "2g"
      cpu: 1
  taskManager:
    replicas: 3
    resource:
      memory: "6g"
      cpu: 2
  job:
    jarURI: local:///opt/flink/usrlib/FlinkDemo-1.0-SNAPSHOT.jar
    entryClass: org.example.Main
    args:
      - "/opt/flink/usrlib/config.properties"
    parallelism: 6
    upgradeMode: savepoint
```



##### 通过service开放端口

Flink-application-service.yaml

```yaml
apiVersion: v1
kind: Service
metadata:
  name: application-deployment-rest
  namespace: flink
spec:
  type: NodePort
  ports:
    - port: 8081       # 服务的目标端口
      targetPort: 8081 # Pod 中的容器端口
      nodePort: 30081  # 在节点上的映射端口
  selector:
    app: application-deployment
```

通过Node Ip + 端口 查看Flink Web UI



#### Kubernetes Deployment Commit Mode

#### Session cluster

Flink Session集群作为Kubernetes Deployment来运行的，可以在一个基于K8s 部署的Session Cluster 中运行多个Flink job，在Kubernetes 上部署Flink Session 集群时，一般至少包含三个组件：

- 运行JobManager的Deployment
- 运行TaskManager的Deployment
- 暴露JobManager上的REST和UI端口的Service

#####  Jobmanager No HA 模版

###### jobmanager-session-deployment-non-ha.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink
      component: jobmanager
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      containers:
        - name: jobmanager
          image: apache/flink:1.20.0-scala_2.12
          args: [ "jobmanager" ]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob-server
            - containerPort: 8081
              name: webui
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: localtime #挂载localtime文件，使容器时间与宿主机一致
              mountPath: /etc/localtime
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: config.yaml
                path: config.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: localtime #挂载localtime文件，使容器时间与宿主机一致
          hostPath:
            path: /etc/localtime
            type: ''
```

##### Jobmanager HA 模版

###### jobmanager-session-deployment-ha.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  replicas: 1 # Set the value to greater than 1 to start standby JobManagers
  selector:
    matchLabels:
      app: flink
      component: jobmanager
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      containers:
        - name: jobmanager
          image: apache/flink:1.20.0-scala_2.12
          env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: status.podIP
          # The following args overwrite the value of jobmanager.rpc.address configured in the configuration config map to POD_IP.
          args: ["jobmanager", "$(POD_IP)"]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob-server
            - containerPort: 8081
              name: webui
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: localtime #挂载localtime文件，使容器时间与宿主机一致
              mountPath: /etc/localtime
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      serviceAccountName: flink-service-account # Service account which has the permissions to create, edit, delete ConfigMaps
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: config.yaml
                path: config.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: localtime #挂载localtime文件，使容器时间与宿主机一致
          hostPath:
            path: /etc/localtime
            type: ''
```

##### Taskmanager 模版

###### taskmanager-session-deployment.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-taskmanager
spec:
  replicas: 2
  selector:
    matchLabels:
      app: flink
      component: taskmanager
  template:
    metadata:
      labels:
        app: flink
        component: taskmanager
    spec:
      containers:
        - name: taskmanager
          image: apache/flink:1.20.0-scala_2.12
          args: ["taskmanager"]
          ports:
            - containerPort: 6122
              name: rpc
          livenessProbe:
            tcpSocket:
              port: 6122
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: localtime #挂载localtime文件，使容器时间与宿主机一致
              mountPath: /etc/localtime
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: config.yaml
                path: config.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: localtime #挂载localtime文件，使容器时间与宿主机一致
          hostPath:
            path: /etc/localtime
            type: ''
```

#### Application cluster

#####  Jobmanager No HA 模版

###### jobmanager-application-non-ha.yaml

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-jobmanager
spec:
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      restartPolicy: OnFailure
      containers:
        - name: jobmanager
          image: apache/flink:1.20.0-scala_2.12
          env:
          args: ["standalone-job", "--job-classname", "com.job.ClassName", <optional arguments>, <job arguments>] # optional arguments: ["--job-id", "<job id>", "--jars", "/path/to/artifact1,/path/to/artifact2", "--fromSavepoint", "/path/to/savepoint", "--allowNonRestoredState"]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob-server
            - containerPort: 8081
              name: webui
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: job-artifacts-volume
              mountPath: /opt/flink/usrlib
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: config.yaml
                path: config.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: job-artifacts-volume
          hostPath:
            path: /host/path/to/job/artifacts
```

##### Jobmanager  HA 模版

###### jobmanager-application-ha.yaml

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-jobmanager
spec:
  parallelism: 1 # Set the value to greater than 1 to start standby JobManagers
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      restartPolicy: OnFailure
      containers:
        - name: jobmanager
          image: apache/flink:1.20.0-scala_2.12
          env:
          - name: POD_IP
            valueFrom:
              fieldRef:
                apiVersion: v1
                fieldPath: status.podIP
          # The following args overwrite the value of jobmanager.rpc.address configured in the configuration config map to POD_IP.
          args: ["standalone-job", "--host", "$(POD_IP)", "--job-classname", "com.job.ClassName", <optional arguments>, <job arguments>] # optional arguments: ["--job-id", "<job id>", "--jars", "/path/to/artifact1,/path/to/artifact2", "--fromSavepoint", "/path/to/savepoint", "--allowNonRestoredState"]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob-server
            - containerPort: 8081
              name: webui
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: job-artifacts-volume
              mountPath: /opt/flink/usrlib
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      serviceAccountName: flink-service-account # Service account which has the permissions to create, edit, delete ConfigMaps
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
            items:
              - key: config.yaml
                path: config.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: job-artifacts-volume
          hostPath:
            path: /host/path/to/job/artifacts
```

##### Taskmanager模版

###### taskmanager-job-deployment.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-taskmanager
spec:
  replicas: 2
  selector:
    matchLabels:
      app: flink
      component: taskmanager
  template:
    metadata:
      labels:
        app: flink
        component: taskmanager
    spec:
      containers:
      - name: taskmanager
        image: apache/flink:1.20.0-scala_2.12
        env:
        args: ["taskmanager"]
        ports:
        - containerPort: 6122
          name: rpc
        livenessProbe:
          tcpSocket:
            port: 6122
          initialDelaySeconds: 30
          periodSeconds: 60
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf/
        - name: job-artifacts-volume
          mountPath: /opt/flink/usrlib
        securityContext:
          runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
          items:
          - key: config.yaml
            path: config.yaml
          - key: log4j-console.properties
            path: log4j-console.properties
      - name: job-artifacts-volume
        hostPath:
          path: /host/path/to/job/artifacts
```





