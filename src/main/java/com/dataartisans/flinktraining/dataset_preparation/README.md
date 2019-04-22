### 一封邮件

邮件与邮件之间使用##//##分隔；
邮件的字段与字段之间使用#|#分隔;
MailCount代码中如下即可读取数据。

```java
env.readCsvFile(input)
    // public final static String MAIL_RECORD_DELIM = "##//##";
    .lineDelimiter(MBoxParser.MAIL_RECORD_DELIM)
    // public final static String MAIL_FIELD_DELIM = "#|#";
    .fieldDelimiter(MBoxParser.MAIL_FIELD_DELIM)
    // take the second and the third field
    .includeFields("011")
    .types(String.class, String.class);
```

### field一目了然 011 是取第2和第三列

```text
##//##
<CALuGr6Z8t5ngbMqW9ehjJKe71G-MVDUedJRngB4SXWDnquVOYg@mail.gmail.com>
#|#
2014-07-14-22:32:32
#|#
Henry Saputra <henry.saputra@gmail.com>
#|#
Re: MODERATE for dev@flink.incubator.apache.org
#|#
Hi Martin,

To start participating/ send emails to dev@flink.incubator.apache.org
please send subscribe request first to
dev-subscribe@flink.incubator.apache.org.



Thanks,

- Henry


On Mon, Jul 14, 2014 at 4:23 AM,
<dev-reject-1405337015.11291.ejgagdfpnegdnbgoeeeh@flink.incubator.apache.org>
wrote:
>
> To approve:
>    dev-accept-1405337015.11291.ejgagdfpnegdnbgoeeeh@flink.incubator.apache.org
> To reject:
>    dev-reject-1405337015.11291.ejgagdfpnegdnbgoeeeh@flink.incubator.apache.org
> To give a reason to reject:
> %%% Start comment
> %%% End comment
>
>
>
> ---------- Forwarded message ----------
> From: Martin Neumann <mneumann@spotify.com>
> To: dev@flink.incubator.apache.org
> Cc:
> Date: Mon, 14 Jul 2014 13:23:07 +0200
> Subject: broadcast variable in spargel
> Hej,
>
> I'm using the latest trunk version of Flink and the new JavaAPI.
> I'm writing some graph algorithms that need to know the number of nodes in the graph. To get the number of nodes I run a short count job and then get a DataSet<Long> that I need to give as input to the other calculations.
>
> It works for normal operations:
>
> nodeList.map(new VertexInit()).withBroadcastSet(numNodes, "#Nodes");
>
>
> However when I call the Spargelcode I don't get the option to do .withBroadcastSet
>
> initNodeList.runOperation(VertexCentricIteration.withPlainEdges(edgeList, new VertexRankUpdater(10, 0.85), new RankMessenger(), 100));
>
>
> How do I use broadcast variables in Spargel?
>
>
> cheers Martin
>
#|#
<1405337015.11291.ezmlm@flink.incubator.apache.org>
##//##
```