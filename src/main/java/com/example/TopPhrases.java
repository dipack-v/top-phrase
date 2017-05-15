package com.example;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

@SpringBootApplication
public class TopPhrases {

	public static void main(String[] args) {
		SpringApplication.run(TopPhrases.class, args);
	}
}

@Configuration
@EnableBatchProcessing
class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	public FlatFileItemReader<String> reader() {
		FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
		reader.setResource(new FileSystemResource("keywords.txt"));
		reader.setLineMapper(new PassThroughLineMapper());

		return reader;
	}

	@Bean
	public PhraseItemProcessor processor() {
		return new PhraseItemProcessor();
	}

	@Bean
	public PhraseCountWriter writer() {
		return new PhraseCountWriter();
	}

	@Bean
	public JobExecutionListener listener() {
		return new JobCompletionNotificationListener(new JdbcTemplate(dataSource));
	}

	// tag::jobstep[]
	@Bean
	public Job topPhraseJob() {
		return jobBuilderFactory.get("topPhraseJob").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(step1()).end().build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<String, List<PhraseCount>>chunk(10).faultTolerant()
				.skip(ItemStreamException.class).reader(reader()).processor(processor()).writer(writer()).build();
	}
	// end::jobstep[]
}

class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;

	private Gson gson = new Gson();

	@Autowired
	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results");

			List<PhraseCount> results = jdbcTemplate.query(
					"SELECT phrase, phrase_count FROM phrase_count order by phrase_count desc limit 100000 ",
					new RowMapper<PhraseCount>() {
						@Override
						public PhraseCount mapRow(ResultSet rs, int row) throws SQLException {
							return new PhraseCount(rs.getString(1), rs.getInt(2));
						}
					});

			try {
				writeJsonStream(new FileOutputStream("top-phrase.txt"), results);
			} catch (FileNotFoundException e) {
				log.error(e.getMessage(), e);
			} catch (IOException e) {
				log.error(e.getMessage(), e);
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

class PhraseItemProcessor implements ItemProcessor<String, List<PhraseCount>> {

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Override
	public List<PhraseCount> process(final String str) throws Exception {
		System.out.println(str);
		String[] keywords = str.split("\\|");
		List<PhraseCount> phraseCounts = new ArrayList<PhraseCount>();
		for (int i = 0; i < keywords.length; i++) {
			String keyword = keywords[i].trim();
			PhraseCount pc = new PhraseCount(keyword, 1);
			phraseCounts.add(pc);
		}

		return phraseCounts;
	}

	public void getCount(String phrase) throws IOException {

		List<Integer> counts = jdbcTemplate.queryForList("SELECT phrase_count FROM phrase_count where phrase = ?",
				int.class, phrase);
		if (counts.iterator().hasNext()) {
			Integer count = counts.iterator().next();
			count++;
			jdbcTemplate.update("update phrase_count set phrase_count= ? where phrase = ? ", count, phrase);
		} else {
			jdbcTemplate.update("INSERT INTO phrase_count (phrase, phrase_count) VALUES (?, ?)", phrase, 1);
		}
	}

}

class PhraseCount {
	private final String phrase;
	private final int count;

	public PhraseCount(String word, int count) {
		this.phrase = word;
		this.count = count;
	}

	public String getPhrase() {
		return phrase;
	}

	public int getCount() {
		return count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((phrase == null) ? 0 : phrase.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PhraseCount other = (PhraseCount) obj;
		if (phrase == null) {
			if (other.phrase != null)
				return false;
		} else if (!phrase.equals(other.phrase))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}

class PhraseCountWriter implements ItemWriter<List<PhraseCount>> {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public void write(List<? extends List<PhraseCount>> items) throws Exception {
		for (List<PhraseCount> phraseCounts : items) {
			for (PhraseCount phraseCount : phraseCounts) {
				List<Integer> counts = jdbcTemplate.queryForList(
						"select phrase_count from phrase_count where phrase = ?", int.class, phraseCount.getPhrase());
				if (counts.iterator().hasNext()) {
					Integer count = counts.iterator().next();
					count++;
					jdbcTemplate.update("update phrase_count set phrase_count= ? where phrase = ? ", count,
							phraseCount.getPhrase());
				} else {
					jdbcTemplate.update("insert into phrase_count (phrase, phrase_count) values (?, ?)",
							phraseCount.getPhrase(), 1);
				}
			}
		}

	}
}
