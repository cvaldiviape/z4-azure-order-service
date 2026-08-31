package com.z4greed.order.config;

import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {

  @Bean
  JwtDecoder jwtDecoder(@Value("${app.jwt.secret}") String secret) {
    return NimbusJwtDecoder.withSecretKey(new SecretKeySpec(secret.getBytes(), "HmacSHA256"))
        .macAlgorithm(MacAlgorithm.HS256)
        .build();
  }

  @Bean
  SecurityFilterChain security(HttpSecurity h) throws Exception {
    return h.csrf(AbstractHttpConfigurer::disable)
        .authorizeHttpRequests(a -> a.requestMatchers("/actuator/health").permitAll().anyRequest().authenticated())
        .oauth2ResourceServer(o -> o.jwt(j -> {}))
        .build();
  }

}