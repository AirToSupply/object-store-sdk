## Object Store SDK

Import dependencies

```xml
<dependency>
    <groupId>tech.odes</groupId>
    <artifactId>object-store-sdk</artifactId>
    <version>0.1.0</version>
</dependency>
```

If you use spring boot to build Microservices, you need to add the following configuration in application.yml:

```yaml
s3:
  endpoint: http://127.0.0.1:9977
  credentials: 
    accessKey: admin
    secretKey: admin
  bucket: oss
```

Add configuration class.

```java
@Configuration
public class AmazonS3Config {
    
    @Value("${s3.endpoint}")
    private String endpoint;
 
    @Value("${s3.credentials.accessKey}")
    private String accessKey;
 
    @Value("${s3.credentials.secretKey}")
    private String secretKey;

    @Value("${s3.bucket}")
    private String bucket;
 
    @Bean
    public S3Client client() {
        return new S3Client.Builder()
            .endpoint(endpoint)
            .accessKey(accessKey)
            .secretKey(secretKey)
            .bucket(bucket)
            .build();
    }
}
```



