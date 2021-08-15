package io.appform.eventingester.server;

import com.google.inject.Stage;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.val;
import ru.vyarus.dropwizard.guice.GuiceBundle;

import static io.appform.eventingester.server.Utils.configureMapper;

/**
 *
 */
public class App extends Application<AppConfig> {

    @Override
    public void initialize(Bootstrap<AppConfig> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                                               new EnvironmentVariableSubstitutor(true)));

        bootstrap.addBundle(
                GuiceBundle.<AppConfig>builder()
                        .enableAutoConfig("io.appform.eventingester.server.resources")
                        .modules(new CoreModule())
                        .printDiagnosticInfo()
                        .build(Stage.PRODUCTION));
    }

    @Override
    public void run(AppConfig appConfig, Environment environment) throws Exception {
        val objectMapper = environment.getObjectMapper();
        configureMapper(objectMapper);
    }

    public static void main(String[] args) throws Exception {
        val app = new App();
        app.run(args);
    }
}
