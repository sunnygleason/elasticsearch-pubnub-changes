/*
 * Licensed to ElasticSearch and Shay Banon under one or more contributor license agreements. See
 * the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. ElasticSearch licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.pubnub.changes;

import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Throwables;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.engine.Engine.Create;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle.Listener;
import org.elasticsearch.indices.IndicesService;
import org.json.JSONException;
import org.json.JSONObject;

import com.pubnub.api.Callback;
import com.pubnub.api.Pubnub;
import com.pubnub.api.PubnubError;

/**
 *
 */
public class PubNubChanges {
  private static final ESLogger logger = Loggers.getLogger(PubNubChanges.class);

  private String pubnubPublishKey;
  private String pubnubSubscribeKey;
  private String pubnubSecretKey;
  private String pubnubCipherKey;
  private boolean pubnubUseSsl;
  private String pubnubChannel;
  private String pubnubUuid;

  private volatile Pubnub pubnub;
  private final ElasticSearchPublishingCallback pnCallback = new ElasticSearchPublishingCallback();

  @SuppressWarnings({"unchecked"})
  @Inject
  public PubNubChanges(Settings rawSettings, Client client, IndicesService indicesService) {
    Map<String, Object> settings = rawSettings.getAsStructuredMap();

    if (!settings.containsKey("pubnub")) {
      logger.info("ignoring pubnub changes plugin: 'pubnub' settings key not present");
      return;
    }

    Map<String, Object> pubnubSettings = (Map<String, Object>) settings.get("pubnub");

    if (!XContentMapValues.nodeBooleanValue(pubnubSettings.get("enabled"), false)) {
      logger.info("ignoring pubnub changes plugin: 'pubnub.enabled' is false or not present");
      return;
    }

    pubnubPublishKey = XContentMapValues.nodeStringValue(pubnubSettings.get("publishKey"), null);
    pubnubSubscribeKey =
        XContentMapValues.nodeStringValue(pubnubSettings.get("subscribeKey"), null);
    pubnubSecretKey = XContentMapValues.nodeStringValue(pubnubSettings.get("secretKey"), null);
    pubnubCipherKey = XContentMapValues.nodeStringValue(pubnubSettings.get("cipherKey"), null);
    pubnubUseSsl = XContentMapValues.nodeBooleanValue(pubnubSettings.get("useSsl"), true);
    pubnubChannel = XContentMapValues.nodeStringValue(pubnubSettings.get("channel"), null);
    pubnubUuid = XContentMapValues.nodeStringValue(pubnubSettings.get("uuid"), null);

    if (this.pubnubCipherKey != null && this.pubnubCipherKey.length() > 0) {
      this.pubnub =
          new Pubnub(pubnubPublishKey, pubnubSubscribeKey, pubnubSecretKey, pubnubCipherKey,
              pubnubUseSsl);
    } else {
      this.pubnub = new Pubnub(pubnubPublishKey, pubnubSubscribeKey, pubnubUseSsl);
    }

    if (this.pubnubUuid != null && this.pubnubUuid.length() > 0) {
      this.pubnub.setUUID(this.pubnubUuid);
    }

    logger.info("creating pubnub changes plugin, uuid={}, channel={}", this.pubnub.getUUID(),
        this.pubnubChannel);

    indicesService.indicesLifecycle().addListener(new Listener() {
      @Override
      public void afterIndexShardStarted(IndexShard indexShard) {
        if (indexShard.routingEntry().primary()) {
          synchronized (this) {
            indexShard.indexingService().addListener(
                new PubNubPublishingListener(indexShard.shardId().getIndex()));
          }
        }
      }

    });
  }

  private class PubNubPublishingListener extends IndexingOperationListener {
    private final String indexName;

    private PubNubPublishingListener(String indexName) {
      this.indexName = indexName;
    }

    @Override
    public void postCreate(Create create) {
      if (!create.origin().equals(Origin.PRIMARY)) {
        return;
      }

      logger.trace("create {} {} {}", indexName, create.type(), create.id());

      try {
        JSONObject todo = new JSONObject();

        todo.put("action", "create");
        todo.put("index", indexName);
        todo.put("type", create.type());
        todo.put("id", create.id());
        todo.put("version", Long.toString(create.version()));

        todo.put("_source", JSONObject.stringToValue(create.source().toUtf8()));

        synchronized (pubnub) {
          pubnub.publish(PubNubChanges.this.pubnubChannel, todo, pnCallback);
        }
      } catch (JSONException ex) {
        throw Throwables.propagate(ex);
      }
    }

    @Override
    public void postIndex(Index index) {
      if (!index.origin().equals(Origin.PRIMARY)) {
        return;
      }

      logger.trace("index {} {} {}", indexName, index.type(), index.id());

      try {
        JSONObject todo = new JSONObject();

        todo.put("action", "update");
        todo.put("index", indexName);
        todo.put("type", index.type());
        todo.put("id", index.id());
        todo.put("version", Long.toString(index.version()));

        todo.put("_source", JSONObject.stringToValue(index.source().toUtf8()));

        synchronized (pubnub) {
          pubnub.publish(PubNubChanges.this.pubnubChannel, todo, pnCallback);
        }
      } catch (JSONException ex) {
        throw Throwables.propagate(ex);
      }
    }

    @Override
    public void postDelete(Delete delete) {
      if (!delete.origin().equals(Origin.PRIMARY)) {
        return;
      }

      logger.trace("delete {} {} {}", indexName, delete.type(), delete.id());

      try {
        JSONObject todo = new JSONObject();

        todo.put("action", "delete");
        todo.put("index", indexName);
        todo.put("type", delete.type());
        todo.put("id", delete.id());

        synchronized (pubnub) {
          pubnub.publish(PubNubChanges.this.pubnubChannel, todo, pnCallback);
        }
      } catch (JSONException ex) {
        throw Throwables.propagate(ex);
      }
    }
  }

  private class ElasticSearchPublishingCallback extends Callback {
    @Override
    public void connectCallback(String channel, Object message) {
      logger.info("pubnub service connected");
    }

    @Override
    public void disconnectCallback(String channel, Object message) {
      logger.info("pubnub service disconnected");
    }

    @Override
    public void reconnectCallback(String channel, Object message) {
      logger.info("pubnub service reconnected");
    }

    @Override
    public void errorCallback(String channel, PubnubError error) {
      logger.warn("pubnub service error: {}", error);
    }

    @Override
    public void successCallback(String channel, Object message) {
      logger.trace("pubnub message published: channel={}, message={}", channel, message);
    }
  }
}
