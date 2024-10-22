package com.batch.config;

import com.batch.Entity.Student;
import com.batch.Repository.StudentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final StudentRepository studentRepository;


    /**
     *
     * Reader
     */
    @Bean
    public FlatFileItemReader<Student> itemReader(){
        FlatFileItemReader<Student> itemReader=new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/batch.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMaper());
        return itemReader;
    }

    private LineMapper<Student> lineMaper(){
        DefaultLineMapper<Student> lineMapper=new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("ID","Name","Mail","Age","Status");
        BeanWrapperFieldSetMapper<Student> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Student.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }


    /**
     *
     * Processor
     */
    @Bean
    public StudentProcessor studentProcessor(){
        return new StudentProcessor();
    }


    /**
     *
     * Writer
     */
    @Bean
    public RepositoryItemWriter<Student> itemWriter(){
        RepositoryItemWriter<Student> itemWriter=new RepositoryItemWriter<>();
        itemWriter.setRepository(studentRepository);
        itemWriter.setMethodName("save");
        return itemWriter;
    }


    @Bean
    public Step importStep(){
        return new StepBuilder("csvImport",jobRepository)
                .<Student,Student>chunk(10,platformTransactionManager)
                .reader(itemReader())
                .processor(studentProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job runJob(){
        return new JobBuilder("importStudents",jobRepository)
                .start(importStep())
                .build();
    }
}
