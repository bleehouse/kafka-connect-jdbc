/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Struct;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.Version;

public class JdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

  ErrantRecordReporter reporter;
  DatabaseDialect dialect;
  JdbcSinkConfig config;
  JdbcDbWriter writer;
  int remainingRetries;

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting JDBC Sink task");
    config = new JdbcSinkConfig(props);
    initWriter();
    remainingRetries = config.maxRetries;
    try {
      reporter = context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      // Will occur in Connect runtimes earlier than 2.6
      reporter = null;
    }
  }

  void initWriter() {
    if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
      dialect = DatabaseDialects.create(config.dialectName, config);
    } else {
      dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
    }
    final DbStructure dbStructure = new DbStructure(dialect);
    log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
    writer = new JdbcDbWriter(config, dialect, dbStructure);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }

    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();

    if (config.isChild == true) {
    
	    Iterator iter_records = records.iterator();
	    List<SinkRecord> templist= new ArrayList<SinkRecord>(); 
	        
	    while(iter_records.hasNext()) {
	    	SinkRecord items = (SinkRecord) iter_records.next();
	    	
	    	String iTopic = items.topic();
	    	Integer iPartition = items.kafkaPartition();
	    	Schema iKeySchema = items.keySchema();
	    	Object iKey = items.key();
	    	Long iTimestamp = items.timestamp();
	    	Iterable<Header> iHeader = items.headers();
	    	TimestampType iTimestampType =  items.timestampType();
	    
	    	Object oldValue = items.value();
	    	
	    	List<String> addkeylist= new ArrayList<String>(); 
	    	org.apache.kafka.connect.data.Struct oldstruct =  (Struct) oldValue;
			Iterator fields_iter = oldstruct.schema().fields().iterator();
			
			while(fields_iter.hasNext()) {

				Field tempf =  (Field)fields_iter.next();
				
				if (tempf.name().equalsIgnoreCase("value")) {
					
				} else if (tempf.name().equalsIgnoreCase("key")) {
					
				} else if (tempf.name().equalsIgnoreCase("timestamp")) {
					
				} else {
					addkeylist.add(tempf.name().toString());
				}
			}
			
	    	JSONParser iparser = new JSONParser();
	    	JSONObject ipayloadObj = null;
	    	
	    	try {
	    		ipayloadObj = (JSONObject) iparser.parse(oldstruct.get("value").toString());
	    	} catch (ParseException e1) {
	    		e1.printStackTrace();
	    	}
	      
	    	SchemaBuilder iValueKeyBuilder = SchemaBuilder.struct().name("com.github.castorm.kafka.connect.http.Value");
	    	Iterator json_iter = ipayloadObj.keySet().iterator();
	    	
	    	while(json_iter.hasNext()) {
	    		String key = (String)json_iter.next();
	    		iValueKeyBuilder.field(key, Schema.STRING_SCHEMA);
	    	}
	    	
	    	for (String key : addkeylist) {
	    		iValueKeyBuilder.field(key, Schema.STRING_SCHEMA); 
	    	}

	        Schema iValueSchema = iValueKeyBuilder.build();
	    	Struct iStruct = new Struct(iValueSchema);
	    	
	    	Iterator iIter_value = ipayloadObj.keySet().iterator();
	    	
	    	while(iIter_value.hasNext()) {
	    		String key = (String)iIter_value.next();
	    		iStruct.put(iValueSchema.field(key),ipayloadObj.get(key));
	    	}    	
	    	
	    	for (String key : addkeylist) {
	    		iStruct.put(iValueSchema.field(key),oldstruct.get(key).toString());
	    	}
	    	SinkRecord iFinalRecord = new SinkRecord(iTopic, iPartition, iKeySchema, iKey, iValueSchema, iStruct, iTimestamp, iTimestamp, iTimestampType, iHeader);
	    	templist.add(iFinalRecord);
	    }
	    
	    records.clear();
	    
	    for(SinkRecord frecord:templist)  {
	    	records.add(frecord);
	    }
    } 
    try {
      writer.write(records);
    } catch (TableAlterOrCreateException tace) {
      if (reporter != null) {
        unrollAndRetry(records);
      } else {
        throw tace;
      }
    } catch (SQLException sqle) {
      log.warn(
          "Write of {} records failed, remainingRetries={}",
          records.size(),
          remainingRetries,
          sqle
      );
      int totalExceptions = 0;
      for (Throwable e :sqle) {
        totalExceptions++;
      }
      SQLException sqlAllMessagesException = getAllMessagesException(sqle);
      if (remainingRetries > 0) {
        writer.closeQuietly();
        initWriter();
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        throw new RetriableException(sqlAllMessagesException);
      } else {
        if (reporter != null) {
          unrollAndRetry(records);
        } else {
          log.error(
              "Failing task after exhausting retries; "
                  + "encountered {} exceptions on last write attempt. "
                  + "For complete details on each exception, please enable DEBUG logging.",
              totalExceptions);
          int exceptionCount = 1;
          for (Throwable e : sqle) {
            log.debug("Exception {}:", exceptionCount++, e);
          }
          throw new ConnectException(sqlAllMessagesException);
        }
      }
    }
    remainingRetries = config.maxRetries;
  }

  private void unrollAndRetry(Collection<SinkRecord> records) {
    writer.closeQuietly();
    for (SinkRecord record : records) {
      try {
        writer.write(Collections.singletonList(record));
      } catch (TableAlterOrCreateException tace) {
        reporter.report(record, tace);
        writer.closeQuietly();
      } catch (SQLException sqle) {
        SQLException sqlAllMessagesException = getAllMessagesException(sqle);
        reporter.report(record, sqlAllMessagesException);
        writer.closeQuietly();
      }
    }
  }

  private SQLException getAllMessagesException(SQLException sqle) {
    String sqleAllMessages = "Exception chain:" + System.lineSeparator();
    for (Throwable e : sqle) {
      sqleAllMessages += e + System.lineSeparator();
    }
    SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
    sqlAllMessagesException.setNextException(sqle);
    return sqlAllMessagesException;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  public void stop() {
    log.info("Stopping task");
    try {
      writer.closeQuietly();
    } finally {
      try {
        if (dialect != null) {
          dialect.close();
        }
      } catch (Throwable t) {
        log.warn("Error while closing the {} dialect: ", dialect.name(), t);
      } finally {
        dialect = null;
      }
    }
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

}
