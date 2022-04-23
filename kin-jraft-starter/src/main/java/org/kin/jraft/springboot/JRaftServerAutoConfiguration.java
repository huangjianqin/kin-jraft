package org.kin.jraft.springboot;

import org.kin.jraft.*;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2021/11/24
 */
@Configuration
@ConditionalOnBean(JRaftServerMarkerConfiguration.Marker.class)
@EnableConfigurationProperties(RaftServerProperties.class)
public class JRaftServerAutoConfiguration {
    @Resource
    private RaftServerProperties serverOptions;

    @Bean
    public RaftServer raftServer(@Autowired(required = false) RaftServiceFactory raftServiceFactory,
                                 @Autowired List<NodeStateChangeListener> listeners,
                                 @Autowired List<RaftGroupOptions> raftGroupOptionsList,
                                 @Autowired(required = false) NodeOptionsCustomizer nodeOptionsCustomizer) {
        RaftServerOptions.Builder builder = RaftServerOptions.builder();
        serverOptions.fillRaftServerOptions(builder);
        if (Objects.nonNull(raftServiceFactory)) {
            builder.raftServiceFactory(raftServiceFactory);
        }
        builder.listeners(listeners);
        builder.groups(raftGroupOptionsList);
        if (Objects.nonNull(nodeOptionsCustomizer)) {
            builder.nodeOptionsCustomizer(nodeOptionsCustomizer);
        }
        return builder.build();
    }
}
