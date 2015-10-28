package org.cloudburst.etl;

import org.cloudburst.etl.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TransformerWorker implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(TransformerWorker.class);

	private String fileName;
	private AtomicInteger counter;

	public TransformerWorker(String fileName,  AtomicInteger counter) {
		this.fileName = fileName;
		this.counter = counter;
	}

	@Override
	public void run() {
		logger.info("Processing file {}", fileName);
		try  {
			processFile();
		} catch (Throwable ex) {
			logger.error("Problem processing file=" + fileName, ex);
		}
		logger.info("Done processing file {}", fileName);
		counter.incrementAndGet();
	}

	private void processFile() {
		String line;
		int bigCount = 1;

		logger.info("Reading file={}", fileName);
		try (BufferedReader inputReader = new BufferedReader(new FileReader(fileName));
			 FileOutputStream fileOutputStream = new FileOutputStream("transformed-" +  fileName)) {
			while ((line = inputReader.readLine()) != null) {
				StringBuilder totalBuilder = new StringBuilder();
				String[] tokens = line.split("\t");

				totalBuilder.append(tokens[0]);
				totalBuilder.append(",");
				totalBuilder.append(tokens[1]);
				totalBuilder.append(",");
				totalBuilder.append(tokens[2]);
				totalBuilder.append(",");
				totalBuilder.append(tokens[3]);
				totalBuilder.append(",");

				StringBuilder textBuilder = new StringBuilder();

				for (int i = 4; i < tokens.length;i++) {
					textBuilder.append(tokens[i]);
				}
				totalBuilder.append(StringUtil.bytesToHex(textBuilder.toString().getBytes()));
				fileOutputStream.write(totalBuilder.toString().getBytes());
				if (bigCount % 50000 == 0) {
					logger.info("file={}, count={}", fileName, bigCount);
				}
				bigCount++;
			}
		} catch (IOException ex) {
			logger.error("Problem reading file=" + fileName, ex);
		}
		logger.info("Done reading file={}", fileName);
	}

}
