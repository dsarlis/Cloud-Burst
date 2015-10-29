package org.cloudburst.etl;

import org.cloudburst.etl.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread to transform a file into another.
 */
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

	/**
	 * Transform fields in the file and write the result into a file.
	 */
	private void processFile() {
		String line;
		int bigCount = 1;

		logger.info("Reading file={}", fileName);
		try (BufferedReader inputReader = new BufferedReader(new FileReader(fileName));
			 FileOutputStream fileOutputStream = new FileOutputStream("transformed-" +  fileName)) {
			SimpleDateFormat timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat gtmFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

			while ((line = inputReader.readLine()) != null) {
				StringBuilder totalBuilder = new StringBuilder();
				String[] tokens = line.split("\t");

				totalBuilder.append(tokens[0]);
				totalBuilder.append(",");
				totalBuilder.append(tokens[1]);
				totalBuilder.append(",");
				totalBuilder.append(timeStampFormat.format(gtmFormat.parse(tokens[2])));
				totalBuilder.append(",");
				totalBuilder.append(tokens[3]);
				totalBuilder.append(",");
				totalBuilder.append(tokens[4]);
				totalBuilder.append(",");
				StringBuilder textBuilder = new StringBuilder();

				for (int i = 5; i < tokens.length;i++) {
					textBuilder.append(tokens[i].replace("\\n", "\n").replace("\\r", "\r"));
					if (i < tokens.length - 1) {
						textBuilder.append("\t");
					}
				}
				totalBuilder.append(StringUtil.bytesToHex(textBuilder.toString().getBytes()));
				totalBuilder.append("\n");
				fileOutputStream.write(totalBuilder.toString().getBytes());
				if (bigCount % 50000 == 0) {
					logger.info("file={}, count={}", fileName, bigCount);
				}
				bigCount++;
			}
		} catch (IOException ex) {
			logger.error("Problem reading file=" + fileName, ex);
		} catch (ParseException e) {
			logger.error("Problem parsing=" + fileName);
		}
		logger.info("Done reading file={}", fileName);
	}

}
