# Ramdom Forest Classification Model

## Prerequisites

- Scala `3.3.0` and Apache Spark `3.4.1` installed
- Setup env var `SPARK_HOME` and `HADOOP_HOME`
- Install [`winutils.exe`](https://github.com/steveloughran/winutils) for Windows

## Usage

Recommended to use IntelliJ IDEA with Scala nightly plugin to run this project.

- Add JVM options to the configuration:
    ```bash
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/java.io=ALL-UNNAMED
    --add-opens=java.base/java.net=ALL-UNNAMED
    --add-opens=java.base/java.nio=ALL-UNNAMED
    --add-opens=java.base/java.util=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
    --add-opens=java.base/sun.security.action=ALL-UNNAMED
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
    ```
- Prepare data in `data` folder
- Invoke `run` function in `src/main/scala/Main.scala`
