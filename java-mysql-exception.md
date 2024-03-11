## 测试

1. 生成测试日志

```sh
docker run -d --name=test-log --entrypoint=/bin/sh -v /tmp/fluentd/java-mysql-exception.log:/tmp/test.log busybox -c "cat /tmp/test.log && sleep 36000"
```

2. 部署fluentd服务

```xml
@include 'prometheus.conf'

<label @FLUENT_LOG>
  <match fluent.**>
    @type null
    @id ignore_fluent_logs
  </match>
</label>

<source>
  @type tail
  @id in_tail_container_logs
  path '/tmp/test.log'
  pos_file /var/log/fluentd-kafka-containers.log.pos
  tag 'kubernetes.*'
  exclude_path use_default
  read_from_head true
  follow_inodes true
  <parse>
    @type "json"
    time_format "%Y-%m-%dT%H:%M:%S.%NZ"
  </parse>
</source>

<filter kubernetes.**>
  @type record_transformer
  <record>
    cluster_id 'hda'
  </record>
</filter>

<match kubernetes.**>
  @type detect_exceptions
  remove_tag_prefix kubernetes
  message log
  languages java, python
  multiline_flush_interval 0.1
</match>

<match **>
  @type stdout
</match>
```

```sh
docker run -d --name=fluentd \
    -v /tmp/fluentd/exception_detector.rb:/fluentd/vendor/bundle/ruby/3.1.0/gems/fluent-plugin-detect-exceptions-0.0.14/lib/fluent/plugin/exception_detector.rb \
    -v /tmp/fluentd/fluent.conf:/fluentd/etc/fluent.conf \
    -v /data/docker/containers/d4151fc13c06f3afe539f0a5ef47d8a7f366a23acb20d8c4244d85ca81d6b325/d4151fc13c06f3afe539f0a5ef47d8a7f366a23acb20d8c4244d85ca81d6b325-json.log:/tmp/test.log \
    fluent/fluentd-kubernetes-daemonset:v1.15.2-debian-kafka2-1.0
```

通过查看fluentd容器的stdout日志判断日志合并情况.

经查看分析, 对比原日志, 发现可以按照设计的规则进行合并, 测试通过!

## 部署

1. 创建配置字典

```
kubectl -n logging-kafka create configmap exception_detector_rb --from-file=exception_detector.rb=/tmp/exception_detector.rb
```

2. 修改fluentd配置清单

```yaml
        volumeMounts:
...
        - mountPath: /fluentd/vendor/bundle/ruby/3.1.0/gems/fluent-plugin-detect-exceptions-0.0.14/lib/fluent/plugin/exception_detector.rb
          name: exception_detector_rb
          subPath: exception_detector.rb
...
      volumes:
...
      - configMap:
          defaultMode: 420
          name: exception_detector_rb
        name: exception_detector_rb
...
```

**注意**: 确保使用的镜像是: `fluent/fluentd-kubernetes-daemonset:v1.15.2-debian-kafka2-1.0`, 不同镜像源代码路径可能有所不同.