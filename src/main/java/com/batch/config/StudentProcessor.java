package com.batch.config;

import com.batch.Entity.Student;
import org.springframework.batch.item.ItemProcessor;

public class StudentProcessor implements ItemProcessor<Student,Student> {
    @Override
    public Student process(Student student) throws Exception {
        student.setName(student.getName().toUpperCase());
        return student;
    }
}
