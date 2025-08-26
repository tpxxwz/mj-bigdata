package com.mj.bean;

import lombok.Data;

@Data
public class Emp {

    private Integer empno;
    private String ename;
    private String job;
    private Integer mgr;
    private Double hiredate;
    private Double sal;
    private Double comm;
    private Integer deptno;

}
