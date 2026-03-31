instance already open commit -> march 23 2026
#############################################################################
janusGraphInstanceManagerServiceImpl.java‎
####################################################################################
+import java.util.Optional;
+import java.util.Properties;
+import java.util.UUID;
+import java.nio.file.Files;
+import java.nio.file.Path;
+import java.nio.file.Paths;
+import java.io.InputStream;
+import java.io.IOException;



  -      graphs.put(instance.toString(), JanusGraphFactory.open(properties));
  +      graphs.put(instance.toString(), openGraphWithUniqueInstanceId(properties));
   + }

   + private JanusGraph openGraphWithUniqueInstanceId(String topologyPropertiesPath) {
    +    Path sourcePath = Paths.get(topologyPropertiesPath);
        if (!Files.exists(sourcePath)) {
    +       throw new IllegalArgumentException("JanusGraph properties file not found: " + topologyPropertiesPath);
     +   }

      +  Properties properties = new Properties();
     +   try (InputStream inputStream = Files.newInputStream(sourcePath)) {
     +       properties.load(inputStream);
     +   } catch (IOException e) {
            throw new IllegalStateException("Failed to load JanusGraph properties: " + topologyPropertiesPath, e);
      +  }

      +  String runtimeUniqueInstanceId = buildRuntimeUniqueInstanceId();
       + properties.setProperty("graph.unique-instance-id", runtimeUniqueInstanceId);
       + logger.info("Using runtime JanusGraph instance-id for instance manager graph: {}", runtimeUniqueInstanceId);

        +JanusGraphFactory.Builder builder = JanusGraphFactory.build();
       + for (String key : properties.stringPropertyNames()) {
            builder.set(key, properties.getProperty(key));
       + }
     +   return builder.open();
    }

    private String buildRuntimeUniqueInstanceId() {
        String hostName = Optional.ofNullable(System.getenv("HOSTNAME"))
                .filter(value -> !value.isBlank())
                .orElse("topology-service");
        return hostName + "-" + UUID.randomUUID();
        
        
####################################################################################
janusgraphServiceImp.java
####################################################################################        
+ import java.io.InputStream;
+ import java.nio.file.Files;
+ import java.nio.file.Path;
+ import java.nio.file.Paths;

+ import java.util.Properties;
+ import java.util.UUID;

- this.graph = JanusGraphFactory.open(topologyProperties);
+            this.graph = openJanusGraphWithUniqueInstanceId(topologyProperties);




+  private JanusGraph openJanusGraphWithUniqueInstanceId(String topologyPropertiesPath) {+           
+        Path sourcePath = Paths.get(topologyPropertiesPath);
+          if (!Files.exists(sourcePath)) {
+               throw new JanusGraphInitializationException(
+                       "TOPOLOGY_PROPERTIES does not point to a readable file: " + topologyPropertiesPath);
+           }
+   
+       Properties properties = new Properties();
+        try (InputStream inputStream = Files.newInputStream(sourcePath)) {
+            properties.load(inputStream);
+        } catch (IOException ioException) {
+            throw new JanusGraphInitializationException(
+                   "Failed to read JanusGraph properties from: " + topologyPropertiesPath, ioException);
+      }

+      String runtimeUniqueInstanceId = buildRuntimeUniqueInstanceId();
+      properties.setProperty("graph.unique-instance-id", runtimeUniqueInstanceId);
+     logger.info("Using runtime JanusGraph instance-id at startup: {}", runtimeUniqueInstanceId);

+      try {
+           JanusGraphFactory.Builder builder = JanusGraphFactory.build();
+           for (String key : properties.stringPropertyNames()) {
+               builder.set(key, properties.getProperty(key));
+           }
+           return builder.open();
+       } catch (Exception openException) {
+          throw new JanusGraphInitializationException("Failed to open JanusGraph with startup properties.",
+                   openException);
+       }
+   }

+   private String buildRuntimeUniqueInstanceId() {
+       String hostName = Optional.ofNullable(System.getenv("HOSTNAME"))
+               .filter(value -> !value.isBlank())
+              .orElse("topology-service");
+       return hostName + "-" + UUID.randomUUID();
+   }
