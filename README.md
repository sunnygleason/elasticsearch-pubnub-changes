PubNub Changes Plugin for ElasticSearch
==================================

The PubNub Changes plugin allows ElasticSearch index changes to be propagated via PubNub.

In order to install the plugin, simply run: `bin/plugin -install sunnygleason/elasticsearch-pubnub-changes/0.0.1`.

    --------------------------------------
    | PubNub Plugin   | ElasticSearch    |
    --------------------------------------
    | master          | 1.3.4 -> master  |
    --------------------------------------
    | 0.0.1           | 1.3.4 -> master  |
    --------------------------------------

PubNub Changes allows ElasticSearch to automatically publish content via PubNub data stream network.

Installing the PubNub Changes Plugin is as simple as editing the elasticsearch.yml:

    pubnub.publishKey: "demo"
    pubnub.subscribeKey: "demo"
    pubnub.useSsl: true
    pubnub.channel: "elasticsearch_publish"

The changes plugin will publish index changes directly to the specified PubNub channel in a format
that is suitable for use by other replication plugins (such as ElasticSearch PubNub River). 
