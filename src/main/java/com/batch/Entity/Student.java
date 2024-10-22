package com.batch.Entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "STUDENTS")
public class Student {
    @Id
    @GeneratedValue
    @Column(name = "ID")
    private Long id;
    @Column(name = "NAME")
    private String name;
    @Column(name = "EMAIL")
    private String email;
    @Column(name = "AGE")
    private Long age;
    @Column(name = "STATUS")
    private String status;
}
