package com.example;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;

	private Gson gson = new Gson();
	@Autowired
	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/*
	 * @Value("${send.from.email}") private String fromEmail;
	 * 
	 * @Value("${send.to.email}") private String toEmail;
	 * 
	 * @Autowired private MailSender mailSender;
	 * 
	 * private SimpleMailMessage templateMessage;
	 */

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results");

			List<PhraseCount> results = jdbcTemplate.query("SELECT phrase, phrase_count FROM phrase_count order by phrase_count desc limit 100000 ",
					new RowMapper<PhraseCount>() {
						@Override
						public PhraseCount mapRow(ResultSet rs, int row) throws SQLException {
							return new PhraseCount(rs.getString(1), rs.getInt(2));
						}
					});

			try {
				writeJsonStream(new FileOutputStream("D:\\output1.txt"), results);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void writeJsonStream(OutputStream out, List<PhraseCount> messages) throws IOException {
		JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
		writer.setIndent("  ");
		writer.beginArray();
		for (PhraseCount message : messages) {
			gson.toJson(message, PhraseCount.class, writer);
		}
		writer.endArray();
		writer.close();
	}

}
