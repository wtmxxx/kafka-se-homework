/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.metadata.bootstrap;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The bootstrap metadata. On startup, if the metadata log is empty, we will populate the log with
 * these records. Alternately, if log is not empty, but the metadata version is not set, we will
 * use the version specified here.
 */
public class BootstrapMetadata {
    private final List<ApiMessageAndVersion> records;
    private final short metadataVersionLevel;
    private final String source;

    public static BootstrapMetadata fromVersions(
        MetadataVersion metadataVersion,
        Map<String, Short> featureVersions,
        String source
    ) {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(metadataVersion.featureLevel()), (short) 0));
        List<String> featureNames = new ArrayList<>(featureVersions.size());
        featureVersions.keySet().forEach(n -> {
            // metadata.version is handled in a special way, and kraft.version generates no
            // FeatureLevelRecord.
            if (!(n.equals(MetadataVersion.FEATURE_NAME) ||
                    n.equals(KRaftVersion.FEATURE_NAME))) {
                featureNames.add(n);
            }
        });
        featureNames.sort(String::compareTo);
        for (String featureName : featureNames) {
            short level = featureVersions.get(featureName);
            if (level > 0) {
                records.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                    setName(featureName).
                    setFeatureLevel(level), (short) 0));
            }
        }
        return new BootstrapMetadata(records, metadataVersion.featureLevel(), source);
    }

    public static BootstrapMetadata fromVersion(MetadataVersion metadataVersion, String source) {
        List<ApiMessageAndVersion> records = List.of(
            new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(metadataVersion.featureLevel()), (short) 0));
        return new BootstrapMetadata(records, metadataVersion.featureLevel(), source);
    }

    public static BootstrapMetadata fromRecords(List<ApiMessageAndVersion> records, String source) {
        Optional<Short> metadataVersionLevel = Optional.empty();
        for (ApiMessageAndVersion record : records) {
            Optional<Short> level = recordToMetadataVersionLevel(record.message());
            if (level.isPresent()) {
                metadataVersionLevel = level;
            }
        }
        if (metadataVersionLevel.isEmpty()) {
            throw new RuntimeException("No FeatureLevelRecord for " + MetadataVersion.FEATURE_NAME +
                    " was found in the bootstrap metadata from " + source);
        }
        return new BootstrapMetadata(records, metadataVersionLevel.get(), source);
    }

    public static Optional<Short> recordToMetadataVersionLevel(ApiMessage record) {
        if (record instanceof FeatureLevelRecord featureLevel) {
            if (featureLevel.name().equals(MetadataVersion.FEATURE_NAME)) {
                return Optional.of(featureLevel.featureLevel());
            }
        }
        return Optional.empty();
    }

    BootstrapMetadata(
        List<ApiMessageAndVersion> records,
        short metadataVersionLevel,
        String source
    ) {
        this.records = Objects.requireNonNull(records);
        this.metadataVersionLevel = metadataVersionLevel;
        Objects.requireNonNull(source);
        this.source = source;
    }

    public List<ApiMessageAndVersion> records() {
        return records;
    }

    public MetadataVersion metadataVersion() {
        return MetadataVersion.fromFeatureLevel(metadataVersionLevel);
    }

    public String source() {
        return source;
    }

    public short featureLevel(String featureName) {
        short result = 0;
        for (ApiMessageAndVersion record : records) {
            if (record.message() instanceof FeatureLevelRecord message) {
                if (message.name().equals(featureName)) {
                    result = message.featureLevel();
                }
            }
        }
        return result;
    }

    public BootstrapMetadata copyWithFeatureRecord(String featureName, short level) {
        List<ApiMessageAndVersion> newRecords = new ArrayList<>();
        int i = 0;
        while (i < records.size()) {
            if (records.get(i).message() instanceof FeatureLevelRecord record) {
                if (record.name().equals(featureName)) {
                    FeatureLevelRecord newRecord = record.duplicate();
                    newRecord.setFeatureLevel(level);
                    newRecords.add(new ApiMessageAndVersion(newRecord, (short) 0));
                    break;
                } else {
                    newRecords.add(records.get(i));
                }
            }
            i++;
        }
        if (i == records.size()) {
            FeatureLevelRecord newRecord = new FeatureLevelRecord().
                setName(featureName).
                setFeatureLevel(level);
            newRecords.add(new ApiMessageAndVersion(newRecord, (short) 0));
        }
        return BootstrapMetadata.fromRecords(newRecords, source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, metadataVersionLevel, source);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        BootstrapMetadata other = (BootstrapMetadata) o;
        return Objects.equals(records, other.records) &&
            metadataVersionLevel == other.metadataVersionLevel &&
            source.equals(other.source);
    }

    @Override
    public String toString() {
        return "BootstrapMetadata(records=" + records +
            ", metadataVersionLevel=" + metadataVersionLevel +
            ", source=" + source +
            ")";
    }
}
