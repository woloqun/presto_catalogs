/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.presto.connector.ConnectorManager;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import okhttp3.*;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.util.Objects.requireNonNull;

public class StaticCatalogStore
{
    private static final Logger log = Logger.get(StaticCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private Announcer announcer;
    private Map<String,String> catalogMap = new HashMap<>();
    public static final String c_split = "\1";
    public static final String f_split = "\2";

    @Inject
    public StaticCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config,Announcer announcer)
    {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()),announcer);
    }

    public StaticCatalogStore(ConnectorManager connectorManager,
                              File catalogConfigurationDir,
                              List<String> disabledCatalogs,
                              Announcer announcer)
    {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.announcer = requireNonNull(announcer, "announcer is null");
    }

    public boolean areCatalogsLoaded()
    {
        return catalogsLoaded.get();
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }

        catalogsLoaded.set(true);

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                log.info("catalogMap:"+catalogMap);
                Map<String, String> newCatalog = new HashMap<>();
                Map<String, String> updateCatalog = new HashMap<>();
                List<String> remoteCatalog = new ArrayList<>();

                Set<String> strings = null;
                try {

                    String catalogStr = catalogstr();
                    String[] catalogs = catalogStr.split(c_split);
                    for (String catalogLine : catalogs) {
                        String[] catalogInfo = catalogLine.split(f_split);
                        String catalogName = catalogInfo[0];
                        remoteCatalog.add(catalogName);
                        if (catalogMap.get(catalogName)==null) {
                            //add catalog
                            newCatalog.put(catalogName, catalogLine);
                            catalogMap.put(catalogName, catalogLine);
                        }else if( catalogMap.get(catalogName)!=null&&!catalogMap.get(catalogName).equals(catalogLine)){
                            //update catalog
                            updateCatalog.put(catalogName, catalogLine);
                        }
                    }



                    strings = newCatalog.keySet();
                    log.info("add catalog:"+strings);
                    for (String catalogname : strings) {
                        addCatalog(newCatalog.get(catalogname));
                    }


                    log.info("update catalog:"+updateCatalog);
                    for (String catalogname : updateCatalog.keySet()) {
                        updateCatalog(updateCatalog.get(catalogname));
                        catalogMap.put(catalogname,updateCatalog.get(catalogname));
                    }

                    List<String> deleteCatalog = catalogMap.keySet().stream().filter(x->!remoteCatalog.contains(x)).collect(Collectors.toList());

                    log.info("delete catalog:"+deleteCatalog);
                    for (String catalogname : deleteCatalog) {
                        deleteCatalog(catalogMap.get(catalogname));
                        catalogMap.remove(catalogname);
                    }


                    newCatalog.clear();
                    updateCatalog.clear();
                    deleteCatalog.clear();

                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }, 1 , 600_000);

    }

    private void deleteCatalog(String catalogLine){
        log.info("deleteCatalog:"+catalogLine);
        if(catalogLine!=null){
            String[] catalogInfo = catalogLine.split(f_split);
            String catalogName = catalogInfo[0];
            connectorManager.dropConnection(catalogName);
            updateConnectorIds(announcer,catalogName,CataLogAction.DELETE);
        }

    }


    private void addCatalog(String catalogLine ){
        log.info("addCatalog:"+catalogLine);

        String[] catalogInfo = catalogLine.split(f_split);
        String catalogName = catalogInfo[0];
        String connectorName = "mysql";
        Map<String, String> properties = new HashMap<>();
        properties.put("connection-url",catalogInfo[1]);
        properties.put("connection-user",catalogInfo[2]);
        properties.put("connection-password",catalogInfo[3]);
        connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(properties));
        updateConnectorIds(announcer,catalogName,CataLogAction.ADD);
        log.info("--------------add or update catalog ------------------"+catalogLine);
    }

    private void updateCatalog(String catalogLine){
        log.info("updateCatalog:"+catalogLine);
        deleteCatalog(catalogLine);
        addCatalog(catalogLine);
    }


    private String catalogstr()throws IOException {
        OkHttpClient client = new OkHttpClient();
        Request.Builder builder = new Request.Builder();
        Request request = builder.url("https://localhost:8777/v1/catalog").build();
        HttpUrl.Builder urlBuilder = request.url().newBuilder();
        Headers.Builder headerBuilder = request.headers().newBuilder();

        builder.url(urlBuilder.build()).headers(headerBuilder.build());
        Response execute = client.newCall(builder.build()).execute();
        return execute.body().string();

    }

    private void loadCatalog(File file)
            throws Exception
    {
        String catalogName = Files.getNameWithoutExtension(file.getName());
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", file);
        Map<String, String> properties = new HashMap<>(loadProperties(file));

        String connectorName = properties.remove("connector.name");
        if("mysql".equals(connectorName)){
            StringBuilder sb = new StringBuilder();
            sb.append(catalogName).append(f_split)
                    .append(properties.get("connection-url")).append(f_split)
                    .append(properties.get("connection-user")).append(f_split)
                    .append(properties.get("connection-password"));
            catalogMap.put(catalogName, sb.toString());
        }

        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

        connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }


    private  void updateConnectorIds(Announcer announcer,String catalogName,CataLogAction action )
    {
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        String property = nullToEmpty(announcement.getProperties().get("connectorIds"));
        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        Set<String> connectorIds = new LinkedHashSet<>(values);
        if(CataLogAction.DELETE.equals(action)){
            connectorIds.remove(catalogName);
        }else{
            connectorIds.add(catalogName);
        }

        ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Map.Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }



    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }

    public enum CataLogAction{
        ADD,DELETE,UPDATE
    }

}