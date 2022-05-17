I have created the project with the Given Requirement and used MultiThreading .
I could not complete the adding it to the DB Part as it has taken more than 2 hours for me to complete this.
But the DB part can be done using the below steps

a) Adding Dependicies in pom.xml
   <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
    <version>2.2.2.RELEASE</version>
</dependency>
<dependency>
    <groupId>org.hsqldb</groupId>
    <artifactId>hsqldb</artifactId>
    <version>2.4.0</version>
    <scope>runtime</scope>
</dependency>



b) Annoating ModelJson class by @Entity

c) Providing the below configuration of the DB on Application Properties files

spring.datasource.driver-class-name=org.hsqldb.jdbc.JDBCDriver 
spring.datasource.url=jdbc:hsqldb:hsql://localhost/testdb 
spring.datasource.username=sa 
spring.datasource.password= 
spring.jpa.hibernate.ddl-auto=update


d) Creating the Repositry like below 

@Repository
public interface LogEventRepo extends CrudRepository<ModelJson, Long> {}
