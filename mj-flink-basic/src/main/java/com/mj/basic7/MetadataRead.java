package com.mj.basic7;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLoader;

import java.util.Collection;

public class MetadataRead {
    public static void main(String[] args) throws Exception {
        String chkPath = "path/to/your/chk/file"; // 例如: hdfs://namenode:8020/flink/checkpoints/chk-123456
        Path path = new Path(chkPath);
        FileSystem fs = path.getFileSystem();
        FSDataInputStream inputStream = fs.open(path);
        /*MetadataHeaders headers = MetadataHeaders.readHeaders(inputStream);
        MetadataInformation metadataInformation = headers.getMetadataInformation();
        inputStream.close();

        System.out.println("Job ID: " + metadataInformation.getJobID());
        System.out.println("Checkpoint ID: " + metadataInformation.getCheckpointId());
        System.out.println("Timestamp: " + metadataInformation.getTimestamp());
        System.out.println("Properties: " + metadataInformation.getProperties());*/
    }
}
