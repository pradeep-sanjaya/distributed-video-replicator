### Video replicator ###
- This program will create a master-server cluster in the environment.
- All new nodes will be connected to the cluster.
- A new master will be elected if the current master node is terminated or killed.
- All nodes will listen for a RabbitMQ message.
- Only the master node will pull content from the S3 bucket, as specified by an attribute in the message.
- The master will broadcast the availability of the new file to the cluster.
- Slave nodes will fetch the content distributed from the master and persist it in the local environment.

### Dependencies ###
| Library | Version| 
| ------ | ------ |
| Java | 1.8 |
| jgroups | 4.1.5 |
| amqp-client | 5.13.0 |
| aws-java-sdk-s3 | 1.12 |

### Publish a message ###
#### Enable RabbitMQ Management Plugin ####
```
rabbitmq-plugins enable rabbitmq_management
```

#### Publish a Message ####
```
curl -i -u guest:guest -H "content-type:application/json" \
-X POST -d'{
  "properties":{},
  "routing_key":"m3u8-queue",
  "payload":"https://<bucket>.s3.amazonaws.com/<file.m3u8>",
  "payload_encoding":"string"
}' http://localhost:15672/api/exchanges/%2f/amq.default/publish
```
