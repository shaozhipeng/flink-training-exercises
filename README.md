The repository contains examples as well as reference solutions and utility classes for the Apache Flink Training exercises 
found on [http://training.ververica.com](http://training.ververica.com).

#### This repo was forked by [ververica/...](https://github.com/ververica/flink-training-exercises)

#### Input data for training

### 数据集准备，跑通实例

##### Download the Mail Data Set

可以使用gzip解压gz文件，查看数据集格式和结构。  
当然..Please do not decompress or rename the .gz file.

```bash
wget http://training.ververica.com/trainingData/flinkMails.gz
```

邮件记录之间使用##//##进行分隔。每一封邮件记录有6个字段组成，字段之间使用#|#字符进行分隔。 

```text
MessageID  : String // a unique message id
Timestamp  : String // the mail deamon timestamp
Sender     : String // the sender of the mail
Subject    : String // the subject of the mail
Body       : String // the body of the mail (contains linebrakes)
Replied-To : String // the messageID of the mail this mail was replied to 
                    //   (may be “null”)
```

```text
<MessageID>#|#<Timestamp>#|#<Sender>#|#<Subject>#|#<Body>#|#<RepliedTo>##//##<MessageId>#|#TimeStamp>#|#...
```

##### Download the taxi data files

```bash
wget http://training.ververica.com/trainingData/nycTaxiRides.gz
wget http://training.ververica.com/trainingData/nycTaxiFares.gz
```

出租车出行数据有11个字段，数据中包含无效的或者丢失经纬度坐标的数据（经纬度为0.0）。

```text
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
isStart        : Boolean   // TRUE for ride start events, FALSE for ride end events
startTime      : DateTime  // the start time of a ride
endTime        : DateTime  // the end time of a ride,
                           //   "1970-01-01 00:00:00" for start events
startLon       : Float     // the longitude of the ride start location
startLat       : Float     // the latitude of the ride start location
endLon         : Float     // the longitude of the ride end location
endLat         : Float     // the latitude of the ride end location
passengerCnt   : Short     // number of passengers on the ride
```

出租车费用数据包含8个字段

```text
rideId         : Long      // a unique id for each ride
taxiId         : Long      // a unique id for each taxi
driverId       : Long      // a unique id for each driver
startTime      : DateTime  // the start time of a ride
paymentType    : String    // CSH or CRD
tip            : Float     // tip for this ride 小费
tolls          : Float     // tolls for this ride 通行费
totalFare      : Float     // total fare collected
```

##### Download the connected car data files

```bash
wget http://training.ververica.com/trainingData/carInOrder.csv
wget http://training.ververica.com/trainingData/carOutOfOrder.csv
```

数据包含23个字段，其中比较关心的是9个字段

```text
id             : String  // a unique id for each event
car_id         : String  // a unique id for the car
timestamp      : long    // timestamp (milliseconds since the epoch)
longitude      : float   // GPS longitude
latitude       : float   // GPS latitude
consumption    : float   // fuel consumption (liters per hour)
speed          : float   // speed (kilometers per hour)
throttle       : float   // throttle position (%)
engineload     : float   // engine load (%)
```

### flink-training

#### Streaming介绍

##### Streaming与Apache Flink

流处理 Stream Processing  

流是数据在自然界中最真实的存在形式。比如，来自web服务器、股票交易市场、工厂车间里机器上的传感器等发生的事件形成的数据就组成了流。  
当我们分析流数据时，可以对数据流设定相应的边界，将流分为有界的数据流和无界的数据流。

批处理：对有界数据流的处理。在计算出结果之前选择一个完整的数据集，对数据进行排序、全局统计、或者针对全部输入数据生成一个最终的报表等。  
流处理：对无界数据流的处理。通常，数据流在某一时刻，开始是固定的，后续的数据输入是永不停止的，所谓的流处理就是：当数据产生时或者数据到达处理引擎时进行持续处理。

在Flink中，应用程序是由数据流dataflow组成，用户会对这些数据流进行一系列的转换操作。从各种数据源获取数据，经过各种各样的转换，将结果输出到不同的系统中。  

Flink程序可以从消息队列或分布式日志系统中消费实时数据，如从Apache Kafka等。也可以处理大量历史的或者有界的数据。数据的处理结果可以发送或保存到各种各样的系统中，如HDFS、RDBMS等，然后可以提供REST API进行访问。  



