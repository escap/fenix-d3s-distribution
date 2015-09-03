package org.fao.ess.faostat.d3s;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.ZipInputStream;

public class FAOSTAT extends WDSDatasetDao {
    private static final String[][] bulkDownloadsMapping = new String[][] {
            {"GL","default","http://faostat3.fao.org/faostat-bulkdownloads/Emissions_Land_Use_Land_Use_Total_E_All_Data_(Norm).zip"},
            {"QC","default","http://faostat3.fao.org/faostat-bulkdownloads/Production_Crops_E_All_Data_(Norm).zip"},
            {"QA","default","http://faostat3.fao.org/faostat-bulkdownloads/Production_Livestock_E_All_Data_(Norm).zip"},
            {"QL","default","http://faostat3.fao.org/faostat-bulkdownloads/Production_LivestockPrimary_E_All_Data_(Norm).zip"},
            {"OA","default","http://faostat3.fao.org/faostat-bulkdownloads/Population_E_All_Data_(Norm).zip"},
            {"FO","default","http://faostat3.fao.org/faostat-bulkdownloads/Forestry_E_All_Data_(Norm).zip"},
            {"GT","default","http://faostat3.fao.org/faostat-bulkdownloads/Emissions_Agriculture_Agriculture_total_E_All_Data_(Norm).zip"}
    };
    private static final Object[][] csvStructures = new Object[][] {
        {
            "default", new Object[][] { { "country", 0 }, { "item", 2 }, { "element", 4 }, { "year", 6 }, { "unit", 8 }, { "value", 9 }, { "flag", 10 } }
        }
    };

    private boolean initialized = false;

    @Override
    public boolean init() {
        return !initialized;
    }

    @Override
    public void init(Map<String, String> properties) throws Exception {
//        if (!initialized)
//            dataSource.init(properties.get("url"),properties.get("usr"),properties.get("psw"));
        initialized = true;
    }

    @Override
    public Iterator<Object[]> loadData(MeIdentification resource) throws Exception {
        String uid = resource.getUid();
        final String domain = uid.toLowerCase().startsWith("faostat_") ? resource.getUid().substring("faostat_".length()) : null;
        Collection<DSDColumn> columns = ((DSDDataset)resource.getDsd()).getColumns();
        String url = domain!=null ? bulkDownloadsMap.get(domain) : null;
        if (url==null || columns==null || columns.size()<1)
            return new LinkedList<Object[]>().iterator();

        final Iterator<String[]> rawData = loadFile(url);
        final ValueConverter[] conversionMap = getConversionMap(columns);
        final Integer[] corrispondenceMap = getCorrispondenceMap(domain, columns);
        final Integer domainColumnIndex = getDomainColumnIndex(columns);

        rawData.next(); //Skip first row
        return new Iterator<Object[]>() {

            @Override
            public boolean hasNext() {
                return rawData.hasNext();
            }

            @Override
            public Object[] next() {
                String[] rawRow = rawData.next();
                Object[] row = new Object[corrispondenceMap.length];

                if (domainColumnIndex!=null)
                    row[domainColumnIndex] = domain;
                for (int i=0; i<row.length; i++)
                    if (corrispondenceMap[i]!=null)
                        row[i] = conversionMap[i].apply(rawRow[corrispondenceMap[i]]);

                return row;
            }

            @Override
            public void remove() {
                rawData.remove();
            }
        };
    }



    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException();
    }

    //LOGIC

    //Data structures
    private static final Map<String, String> bulkDownloadsMap = new HashMap<>();
    static {
        for (String[] mapping : bulkDownloadsMapping)
            bulkDownloadsMap.put(mapping[0], mapping[2]);
    }

    private static final Map<String,Map<String,Integer>> structuresMap = new HashMap<>();
    static {
        Map<String,Map<String,Integer>> structures = new HashMap<>();
        for (Object[] csvStructure : csvStructures) {
            Map<String, Integer> structure = new HashMap<>();
            structures.put((String)csvStructure[0],structure);
            for (Object[] csvColumn : ((Object[][])csvStructure[1]))
                structure.put((String)csvColumn[0], (Integer)csvColumn[1]);
        }
        for (String[] mapping : bulkDownloadsMapping)
            structuresMap.put(mapping[0], structures.get(mapping[1]));
    }

    //Retrieve columns conversion map
    private ValueConverter[] getConversionMap (Collection<DSDColumn> columns) throws Exception {
        ValueConverter[] map = new ValueConverter[columns.size()];
        int i=0;
        for (DSDColumn column : columns)
            switch (column.getDataType()) {
                case number: map[i++] = new ValueConverter(ValueType.DOUBLE); break;
                case year: map[i++] = new ValueConverter(ValueType.INT); break;
                default: map[i++] = new ValueConverter(ValueType.STRING);
            }
        return map;
    }

    //Retrieve columns corrispondence
    private Integer[] getCorrispondenceMap (String domain, Collection<DSDColumn> columns) throws Exception {
        Map<String,Integer> csvStructure = structuresMap.get(domain);
        Integer[] map = new Integer[columns.size()];
        int i=0;
        for (DSDColumn column : columns)
            map[i++] = csvStructure.get(column.getId());
        return map;
    }

    //Retrieve domain column index
    private Integer getDomainColumnIndex (Collection<DSDColumn> columns) throws Exception {
        int i=0;
        for (DSDColumn column : columns)
            if (column.getId().equals("domain"))
                return i;
        return null;
    }

    //Load raw data from bulk download
    private org.fao.fenix.commons.utils.database.Iterator<String[]> loadFile(String url) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        ZipInputStream input = new ZipInputStream(new BufferedInputStream(connection.getInputStream(),1024));
        input.getNextEntry();

        return parseFile(new BufferedReader(new InputStreamReader(input)));
    }

    private org.fao.fenix.commons.utils.database.Iterator<String[]> parseFile(BufferedReader input) throws Exception {
        final CSVParser parser = CSVFormat.DEFAULT.withAllowMissingColumnNames().withIgnoreEmptyLines().withIgnoreSurroundingSpaces().withQuote('"').withRecordSeparator(',').parse(input);
        final Iterator<CSVRecord> csvIterator = parser.iterator();


        return new org.fao.fenix.commons.utils.database.Iterator<String[]>() {
            String[] currentRow;
            boolean consumed = true;
            long index = 0;

            @Override
            public long getIndex() {
                return index;
            }

            @Override
            public boolean hasNext() {
                if (consumed) {
                    try {
                        CSVRecord csvRecord = !parser.isClosed() && csvIterator.hasNext() ? csvIterator.next() : null;
                        if (csvRecord == null) {
                            parser.close();
                            return false;
                        }

                        currentRow = new String[csvRecord.size()];
                        int i=0;
                        for (String cell : csvRecord)
                            currentRow[i++] = cell;
                        consumed = false;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return true;
            }

            @Override
            public String[] next() {
                if (hasNext()) {
                    consumed = true;
                    index++;
                    return currentRow;
                } else
                    return null;
            }

            @Override
            public void skip(long amount) {
            }

            @Override
            public void remove() {
            }
        };
    }

}
