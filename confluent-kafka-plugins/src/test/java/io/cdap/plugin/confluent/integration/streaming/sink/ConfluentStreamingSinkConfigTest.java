/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.confluent.integration.streaming.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.confluent.streaming.sink.ConfluentStreamingSinkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link ConfluentStreamingSinkConfig}.
 */
public class ConfluentStreamingSinkConfigTest {

  private final Schema schema = Schema.recordOf("etlSchemaBody",
                                                Schema.Field.of("message", Schema.of(Schema.Type.STRING)));

  @Test
  public void testValidate() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ConfluentStreamingSinkConfig config = ConfluentStreamingSinkConfigHelper.newConfigBuilder()
      .setReferenceName(ConfluentStreamingSinkConfigHelper.TEST_REF_NAME)
      .setBrokers("hostname:9092")
      .setTopic("topic-1")
      .setFormat("csv")
      .setClusterApiKey(ConfluentStreamingSinkConfigHelper.TEST_CLUSTER_API_KEY)
      .setClusterApiSecret(ConfluentStreamingSinkConfigHelper.TEST_CLUSTER_API_SECRET)
      .build();
    config.validate(schema, mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    Assert.assertEquals(0, validationFailures.size());
  }

  @Test
  public void testValidateWhenClusterAPIKeyIsNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ConfluentStreamingSinkConfig config = ConfluentStreamingSinkConfigHelper.newConfigBuilder()
      .setReferenceName(ConfluentStreamingSinkConfigHelper.TEST_REF_NAME)
      .setBrokers("hostname:9092")
      .setTopic("topic-1")
      .setFormat("csv")
      .setClusterApiKey(null)
      .setClusterApiSecret(ConfluentStreamingSinkConfigHelper.TEST_CLUSTER_API_SECRET)
      .build();
    config.validate(schema, mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    Assert.assertEquals(1, validationFailures.size());
    ValidationFailure getResult = validationFailures.get(0);
    Assert.assertEquals("Cluster API Key should be provided when Cluster API Secret is used.",
                        getResult.getMessage());
  }
  
  @Test
  public void testValidateWhenClusterAPISecretIsNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ConfluentStreamingSinkConfig config = ConfluentStreamingSinkConfigHelper.newConfigBuilder()
      .setReferenceName(ConfluentStreamingSinkConfigHelper.TEST_REF_NAME)
      .setBrokers("hostname:9092")
      .setTopic("topic-1")
      .setFormat("csv")
      .setClusterApiKey(ConfluentStreamingSinkConfigHelper.TEST_CLUSTER_API_KEY)
      .setClusterApiSecret(null)
      .build();
    config.validate(schema, mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    Assert.assertEquals(1, validationFailures.size());
    ValidationFailure getResult = validationFailures.get(0);
    Assert.assertEquals("Cluster API Secret should be provided when Cluster API Key is used.",
                 getResult.getMessage());
  }

  @Test
  public void testValidateWhenBothClusterAPIKeyAndClusterAPISecretAreNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    ConfluentStreamingSinkConfig config = ConfluentStreamingSinkConfigHelper.newConfigBuilder()
      .setReferenceName(ConfluentStreamingSinkConfigHelper.TEST_REF_NAME)
      .setBrokers("hostname:9092")
      .setTopic("topic-1")
      .setFormat("csv")
      .setClusterApiKey(null)
      .setClusterApiSecret(null)
      .build();
    config.validate(schema, mockFailureCollector);
    List<ValidationFailure> validationFailures = mockFailureCollector.getValidationFailures();
    Assert.assertEquals(0, validationFailures.size());
  }
}
