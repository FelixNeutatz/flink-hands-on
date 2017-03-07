package de.tuberlin.dima.flinkhandson.kafka;

import com.google.common.base.Joiner;
import org.apache.flink.streaming.util.StreamingProgramTestBase;

import org.junit.Test;

public class KafkaTest extends StreamingProgramTestBase{


	protected static String expectedOutput[] = {
		"(male,20)",
		"(female,21)",
		"(male,22)",
		"(female,23)",
		"(male,24)",
		"(female,25)",
		"(male,26)",
		"(female,27)",
		"(male,28)",
		"(female,29)"
	};

	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		this.resultPath = this.getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(Joiner.on("\n").join(expectedOutput), this.resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		// We need to remove the file scheme because we can't use the Flink file system.
		// (to remain compatible with Storm)
		RideCleansingToKafka.main(new String[]{ this.resultPath.replace("file:", "") });
	}
}
