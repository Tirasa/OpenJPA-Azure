/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.    
 */
package org.apache.openjpa.jdbc.meta;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.kernel.JDBCSeq;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.schema.SQLAzureSchemaTool;
import org.apache.openjpa.jdbc.schema.Schema;
import org.apache.openjpa.jdbc.schema.SchemaGroup;
import org.apache.openjpa.jdbc.schema.SchemaSerializer;
import org.apache.openjpa.jdbc.schema.SchemaTool;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.schema.XMLSchemaSerializer;
import org.apache.openjpa.jdbc.sql.DBDictionary;
import org.apache.openjpa.kernel.Seq;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.lib.meta.MetaDataSerializer;
import org.apache.openjpa.lib.meta.SourceTracker;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.meta.JavaTypes;
import org.apache.openjpa.meta.MetaDataFactory;
import org.apache.openjpa.meta.QueryMetaData;
import org.apache.openjpa.meta.SequenceMetaData;
import org.apache.openjpa.meta.ValueStrategies;
import org.apache.openjpa.util.GeneralException;
import org.apache.openjpa.util.MetaDataException;

/**
 * Tool for manipulating class mappings and associated schema.
 *
 * @author Abe White
 */
public class SQLAzureMappingTool extends MappingTool {

    private static final Localizer _loc = Localizer.forPackage(SQLAzureMappingTool.class);

    private final JDBCConfiguration _conf;

    private final DBDictionary _dict;

    private final Log _log;

    private SchemaTool _schemaTool = null;

    private final int _mode;

    // buffer metadatas to be dropped
    private Set<Class<?>> _dropCls = null;

    private Set<ClassMapping> _dropMap = null;

    private boolean _flush = false;

    private boolean _flushSchema = false;

    /**
     * Constructor. Supply configuration and action.
     */
    public SQLAzureMappingTool(final JDBCConfiguration conf, final String action, final boolean meta) {
        super(conf, action, meta);

        this._conf = conf;
        _log = conf.getLog(JDBCConfiguration.LOG_METADATA);
        _dict = _conf.getDBDictionaryInstance();

        if (meta && ACTION_ADD.equals(getAction())) {
            _mode = MODE_META;
        } else if (meta && ACTION_DROP.equals(getAction())) {
            _mode = MODE_META | MODE_MAPPING | MODE_QUERY;
        } else {
            _mode = MODE_MAPPING;
        }
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void setSchemaTool(final SchemaTool tool) {
        super.setSchemaTool(tool);
        this._schemaTool = tool;
    }

    /**
     * Return the schema tool to use for schema modification.
     */
    private SchemaTool newSchemaTool(String action) {
        if (SCHEMA_ACTION_NONE.equals(action)) {
            action = null;
        }
        final SchemaTool tool = new SQLAzureSchemaTool(_conf, action);
        tool.setIgnoreErrors(getIgnoreErrors());
        tool.setPrimaryKeys(getPrimaryKeys());
        tool.setForeignKeys(getForeignKeys());
        tool.setIndexes(getIndexes());
        tool.setSequences(getSequences());
        return tool;
    }

    /**
     * Records the changes that have been made to both the mappings and the associated schema, and clears the tool for
     * further use. This also involves clearing the internal mapping repository.
     */
    public void record() {
        record(null);
    }

    public void record(MappingTool.Flags flags) {
        MappingRepository repos = getRepository();
        MetaDataFactory io = repos.getMetaDataFactory();
        ClassMapping[] mappings;

        if (!ACTION_DROP.equals(getAction())) {
            mappings = repos.getMappings();
        } else if (_dropMap != null) {
            mappings = (ClassMapping[]) _dropMap.toArray(new ClassMapping[_dropMap.size()]);
        } else {
            mappings = new ClassMapping[0];
        }

        try {
            if (_dropCls != null && !_dropCls.isEmpty()) {
                Class<?>[] cls = (Class[]) _dropCls.toArray(new Class[_dropCls.size()]);
                if (!io.drop(cls, _mode, null)) {
                    _log.warn(_loc.get("bad-drop", _dropCls));
                }
            }

            if (_flushSchema) {
                // drop portions of the known schema that no mapping uses, and
                // add sequences used for value generation
                if (getDropUnusedComponents()) {
                    dropUnusedSchemaComponents(mappings);
                }
                addSequenceComponents(mappings);

                // now run the schematool as long as we're doing some schema
                // action and the user doesn't just want an xml output
                String[] schemaActions = getSchemaAction().split(",");
                for (int i = 0; i < schemaActions.length; i++) {
                    if (!SCHEMA_ACTION_NONE.equals(schemaActions[i])
                            && (getSchemaWriter() == null || (_schemaTool != null
                            && _schemaTool.getWriter() != null))) {

                        final SQLAzureSchemaTool tool = (SQLAzureSchemaTool) newSchemaTool(schemaActions[i]);

                        // configure the tool with additional settings
                        if (flags != null) {
                            tool.setDropTables(flags.dropTables);
                            tool.setDropSequences(flags.dropSequences);
                            tool.setWriter(flags.sqlWriter);
                            tool.setOpenJPATables(flags.openjpaTables);
                            tool.setSQLTerminator(flags.sqlTerminator);
                        }

                        tool.setSchemaGroup(getSchemaGroup());
                        tool.run();
                        tool.record();
                        tool.clear();
                    }
                }

                // xml output of schema?
                if (getSchemaWriter() != null) {
                    // serialize the planned schema to the stream
                    SchemaSerializer ser = new XMLSchemaSerializer(_conf);
                    ser.addAll(getSchemaGroup());
                    ser.serialize(getSchemaWriter(), MetaDataSerializer.PRETTY);
                    getSchemaWriter().flush();
                }
            }
            if (!_flush) {
                return;
            }

            QueryMetaData[] queries = repos.getQueryMetaDatas();
            SequenceMetaData[] seqs = repos.getSequenceMetaDatas();
            Map<File, String> output = null;

            // if we're outputting to stream, set all metas to same file so
            // they get placed in single string
            if (getMappingWriter() != null) {
                output = new HashMap<File, String>();
                File tmp = new File("openjpatmp");
                for (int i = 0; i < mappings.length; i++) {
                    mappings[i].setSource(tmp, SourceTracker.SRC_OTHER, "openjpatmp");
                }
                for (int i = 0; i < queries.length; i++) {
                    queries[i].setSource(tmp, queries[i].getSourceScope(), SourceTracker.SRC_OTHER, "openjpatmp");
                }
                for (int i = 0; i < seqs.length; i++) {
                    seqs[i].setSource(tmp, seqs[i].getSourceScope(),
                            SourceTracker.SRC_OTHER);
                }
            }

            // store
            if (!io.store(mappings, queries, seqs, _mode, output)) {
                throw new MetaDataException(_loc.get("bad-store"));
            }

            // write to stream
            if (getMappingWriter() != null) {
                PrintWriter out = new PrintWriter(getMappingWriter());
                for (Iterator<String> itr = output.values().iterator();
                        itr.hasNext();) {
                    out.println(itr.next());
                }
                out.flush();
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new GeneralException(e);
        } finally {
            clear();
        }
    }

    /**
     * Drops schema components that appear to be unused from the local copy of the schema group.
     */
    private void dropUnusedSchemaComponents(ClassMapping[] mappings) {
        FieldMapping[] fields;
        for (int i = 0; i < mappings.length; i++) {
            mappings[i].refSchemaComponents();
            mappings[i].getDiscriminator().refSchemaComponents();
            mappings[i].getVersion().refSchemaComponents();
            fields = mappings[i].getDefinedFieldMappings();
            for (int j = 0; j < fields.length; j++) {
                fields[j].refSchemaComponents();
            }
        }

        // also allow the dbdictionary to ref any schema components that
        // it adds apart from mappings
        SchemaGroup group = getSchemaGroup();
        Schema[] schemas = group.getSchemas();
        Table[] tables;
        for (int i = 0; i < schemas.length; i++) {
            tables = schemas[i].getTables();
            for (int j = 0; j < tables.length; j++) {
                _dict.refSchemaComponents(tables[j]);
            }
        }

        group.removeUnusedComponents();
    }

    /**
     * Add tables used by sequences to the given schema.
     */
    private void addSequenceComponents(ClassMapping[] mappings) {
        SchemaGroup group = getSchemaGroup();
        for (int i = 0; i < mappings.length; i++) {
            addSequenceComponents(mappings[i], group);
        }
    }

    /**
     * Add tables used by sequences to the given schema.
     */
    private void addSequenceComponents(ClassMapping mapping,
            SchemaGroup group) {
        SequenceMetaData smd = mapping.getIdentitySequenceMetaData();
        Seq seq = null;
        if (smd != null) {
            seq = smd.getInstance(null);
        } else if (mapping.getIdentityStrategy() == ValueStrategies.NATIVE
                || (mapping.getIdentityStrategy() == ValueStrategies.NONE
                && mapping.getIdentityType() == ClassMapping.ID_DATASTORE)) {
            seq = _conf.getSequenceInstance();
        }

        if (seq instanceof JDBCSeq) {
            ((JDBCSeq) seq).addSchema(mapping, group);
        }

        FieldMapping[] fmds;
        if (mapping.getEmbeddingMetaData() == null) {
            fmds = mapping.getDefinedFieldMappings();
        } else {
            fmds = mapping.getFieldMappings();
        }
        for (int i = 0; i < fmds.length; i++) {
            smd = fmds[i].getValueSequenceMetaData();
            if (smd != null) {
                seq = smd.getInstance(null);
                if (seq instanceof JDBCSeq) {
                    ((JDBCSeq) seq).addSchema(mapping, group);
                }
            } else if (fmds[i].getEmbeddedMapping() != null) {
                addSequenceComponents(fmds[i].getEmbeddedMapping(), group);
            }
        }
    }

    ///////////
    // Actions
    ///////////
    /**
     * Run the configured action on the given instance.
     */
    @Override
    public void run(Class<?> cls) {
        if (ACTION_ADD.equals(getAction())) {
            if (isMetaDataAction()) {
                addMeta(cls);
            } else {
                add(cls);
            }
        } else if (ACTION_REFRESH.equals(getAction())) {
            refresh(cls);
        } else if (ACTION_BUILD_SCHEMA.equals(getAction())) {
            buildSchema(cls);
        } else if (ACTION_DROP.equals(getAction())) {
            drop(cls);
        } else if (ACTION_VALIDATE.equals(getAction())) {
            validate(cls);
        }
    }

    /**
     * Add the mapping for the given instance.
     */
    private void add(Class<?> cls) {
        if (cls == null) {
            return;
        }

        MappingRepository repos = getRepository();
        repos.setStrategyInstaller(new MappingStrategyInstaller(repos));
        if (getMapping(repos, cls, true) != null) {
            _flush = true;
            _flushSchema = true;
        }
    }

    /**
     * Return the mapping for the given type, or null if the type is persistence-aware.
     */
    private static ClassMapping getMapping(MappingRepository repos, Class<?> cls,
            boolean validate) {
        // this will parse all possible metadata rsrcs looking for cls, so
        // will detect if p-aware
        ClassMapping mapping = repos.getMapping(cls, null, false);
        if (mapping != null) {
            return mapping;
        }
        if (!validate || cls.isInterface()
                || repos.getPersistenceAware(cls) != null) {
            return null;
        }
        throw new MetaDataException(_loc.get("no-meta", cls, cls.getClassLoader()));
    }

    /**
     * Create a metadata for the given instance.
     */
    private void addMeta(Class<?> cls) {
        if (cls == null) {
            return;
        }

        _flush = true;
        MappingRepository repos = getRepository();
        repos.setResolve(MODE_MAPPING, false);
        MetaDataFactory factory = repos.getMetaDataFactory();
        factory.getDefaults().setIgnoreNonPersistent(false);
        factory.setStoreMode(MetaDataFactory.STORE_VERBOSE);

        ClassMetaData meta = repos.addMetaData(cls);
        FieldMetaData[] fmds = meta.getDeclaredFields();
        for (int i = 0; i < fmds.length; i++) {
            if (fmds[i].getDeclaredTypeCode() == JavaTypes.OBJECT
                    && fmds[i].getDeclaredType() != Object.class) {
                fmds[i].setDeclaredTypeCode(JavaTypes.PC);
            }
        }
        meta.setSource(
                getMetaDataFile(), meta.getSourceType(), getMetaDataFile() == null ? "" : getMetaDataFile().getPath());
        meta.setResolve(MODE_META, true);
    }

    /**
     * Refresh or add the mapping for the given instance.
     */
    private void refresh(Class<?> cls) {
        if (cls == null) {
            return;
        }

        MappingRepository repos = getRepository();
        repos.setStrategyInstaller(new RefreshStrategyInstaller(repos));
        if (getMapping(repos, cls, true) != null) {
            _flush = true;
            _flushSchema = true;
        }
    }

    /**
     * Validate the mappings for the given class and its fields.
     */
    private void validate(Class<?> cls) {
        if (cls == null) {
            return;
        }

        MappingRepository repos = getRepository();
        repos.setStrategyInstaller(new RuntimeStrategyInstaller(repos));
        if (getMapping(repos, cls, true) != null) {
            _flushSchema = !contains(getSchemaAction(), SCHEMA_ACTION_NONE)
                    && !contains(getSchemaAction(), SchemaTool.ACTION_ADD);
        }
    }

    /**
     * Create the schema using the mapping for the given instance.
     */
    private void buildSchema(Class<?> cls) {
        if (cls == null) {
            return;
        }

        MappingRepository repos = getRepository();
        repos.setStrategyInstaller(new RuntimeStrategyInstaller(repos));
        if (getMapping(repos, cls, true) == null) {
            return;
        }

        // set any logical pks to non-logical so they get flushed
        _flushSchema = true;
        Schema[] schemas = getSchemaGroup().getSchemas();
        Table[] tables;
        Column[] cols;
        for (int i = 0; i < schemas.length; i++) {
            tables = schemas[i].getTables();
            for (int j = 0; j < tables.length; j++) {
                if (tables[j].getPrimaryKey() == null) {
                    continue;
                }

                tables[j].getPrimaryKey().setLogical(false);
                cols = tables[j].getPrimaryKey().getColumns();
                for (int k = 0; k < cols.length; k++) {
                    cols[k].setNotNull(true);
                }
            }
        }
    }

    /**
     * Drop mapping for given class.
     */
    private void drop(Class<?> cls) {
        if (cls == null) {
            return;
        }

        if (_dropCls == null) {
            _dropCls = new HashSet<Class<?>>();
        }
        _dropCls.add(cls);
        if (!contains(getSchemaAction(), SchemaTool.ACTION_DROP)) {
            return;
        }

        MappingRepository repos = getRepository();
        repos.setStrategyInstaller(new RuntimeStrategyInstaller(repos));
        ClassMapping mapping = null;
        try {
            mapping = repos.getMapping(cls, null, false);
        } catch (Exception e) {
        }

        if (mapping != null) {
            _flushSchema = true;
            if (_dropMap == null) {
                _dropMap = new HashSet<ClassMapping>();
            }
            _dropMap.add(mapping);
        } else {
            _log.warn(_loc.get("no-drop-meta", cls));
        }
    }

    private static boolean contains(String list, String key) {
        return (list == null) ? false : list.indexOf(key) != -1;
    }
}
