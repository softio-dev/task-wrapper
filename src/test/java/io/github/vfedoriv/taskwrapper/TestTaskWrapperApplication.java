package io.github.vfedoriv.taskwrapper;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.TestConfiguration;

@TestConfiguration(proxyBeanMethods = false)
public class TestTaskWrapperApplication {

    public static void main(String[] args) {
        SpringApplication.from(TaskWrapperApplication::main).with(TestTaskWrapperApplication.class).run(args);
    }

}
