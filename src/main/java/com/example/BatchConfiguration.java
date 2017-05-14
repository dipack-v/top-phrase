package com.example;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    // tag::readerwriterprocessor[]
    @Bean
    public FlatFileItemReader<String> reader() {
        FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
        reader.setResource(new FileSystemResource("E:\\output\\output.txt"));
        reader.setLineMapper(new PassThroughLineMapper());
 
        return reader;
    }

    @Bean
    public PhraseItemProcessor processor() {
        return new PhraseItemProcessor();
    }

    @Bean
    public FlatFileRecordsWriter writer() {
    	FlatFileItemWriter<PhraseCount> writer = new FlatFileItemWriter<PhraseCount>();
    	writer.setResource(new FileSystemResource("E:\\output.txt"));
    	writer.setLineAggregator(new PassThroughLineAggregator<>());
        return new FlatFileRecordsWriter(writer);
    }
    // end::readerwriterprocessor[]

    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionNotificationListener(new JdbcTemplate(dataSource));
    }

    // tag::jobstep[]
    @Bean
    public Job topPhraseJob() {
        return jobBuilderFactory.get("topPhraseJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener())
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String, List<PhraseCount>> chunk(10).faultTolerant().skip(ItemStreamException.class)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }
    
  
    // end::jobstep[]
}
