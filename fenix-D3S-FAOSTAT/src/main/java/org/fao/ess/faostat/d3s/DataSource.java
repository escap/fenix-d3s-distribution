package org.fao.ess.faostat.d3s;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.fao.fenix.commons.utils.database.Iterator;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.Buffer;
import java.sql.*;
import java.util.Collection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class DataSource {
    static {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    private String url,usr,psw;

    public void init(String url, String usr, String psw) {
        this.url = url;
        this.usr = usr;
        this.psw = psw;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, usr, psw);
    }






/*
    public static void main(String[] args) {
        DataSource ds = new DataSource();
        ds.init(
                "jdbc:sqlserver://HQWPRFAOSTATDB1\\Dissemination;databaseName=Warehouse;",
                "Warehouse",
                "w@reh0use"
        );

        String query = "EXECUTE Warehouse.dbo.usp_GetDataTEST  \n" +
                "@DomainCode = 'QC',   \n" +
                "@lang = 'E',   \n" +
                "@List1Codes = '(''2'',''3'',''4'',''5'',''6'',''7'',''258'',''8'',''9'',''1'',''22'',''10'',''11'',''52'',''12'',''13'',''16'',''14'',''57'',''255'',''15'',''23'',''53'',''17'',''18'',''19'',''80'',''20'',''21'',''239'',''26'',''27'',''233'',''29'',''35'',''115'',''32'',''33'',''36'',''37'',''39'',''259'',''40'',''351'',''96'',''128'',''41'',''214'',''44'',''45'',''46'',''47'',''48'',''107'',''98'',''49'',''50'',''167'',''51'',''116'',''250'',''54'',''72'',''55'',''56'',''58'',''59'',''60'',''61'',''178'',''63'',''238'',''62'',''65'',''64'',''66'',''67'',''68'',''69'',''70'',''74'',''75'',''73'',''79'',''81'',''82'',''84'',''85'',''86'',''87'',''88'',''89'',''90'',''175'',''91'',''93'',''94'',''95'',''97'',''99'',''100'',''101'',''102'',''103'',''104'',''264'',''105'',''106'',''109'',''110'',''112'',''108'',''114'',''83'',''118'',''113'',''120'',''119'',''121'',''122'',''123'',''124'',''125'',''126'',''256'',''129'',''130'',''131'',''132'',''133'',''134'',''127'',''135'',''136'',''137'',''270'',''138'',''145'',''140'',''141'',''273'',''142'',''143'',''144'',''28'',''147'',''148'',''149'',''150'',''151'',''153'',''156'',''157'',''158'',''159'',''160'',''161'',''163'',''162'',''299'',''221'',''164'',''165'',''180'',''166'',''168'',''169'',''170'',''171'',''172'',''173'',''174'',''177'',''179'',''117'',''146'',''182'',''183'',''185'',''184'',''187'',''188'',''189'',''190'',''191'',''244'',''192'',''193'',''194'',''195'',''272'',''186'',''196'',''197'',''200'',''199'',''198'',''25'',''201'',''202'',''277'',''203'',''38'',''276'',''206'',''207'',''260'',''209'',''210'',''211'',''212'',''208'',''216'',''154'',''176'',''217'',''218'',''219'',''220'',''222'',''223'',''213'',''224'',''227'',''226'',''230'',''225'',''229'',''215'',''231'',''240'',''234'',''228'',''235'',''155'',''236'',''237'',''243'',''205'',''249'',''248'',''251'',''181'')',   \n" +
                "@List2Codes = '(''2312'')',   \n" +
                "@List3Codes = '(''800'',''221'',''711'',''515'',''526'',''226'',''366'',''367'',''572'',''203'',''486'',''44'',''782'',''176'',''414'',''558'',''552'',''216'',''181'',''89'',''358'',''101'',''461'',''426'',''217'',''591'',''125'',''378'',''265'',''393'',''108'',''531'',''530'',''220'',''191'',''459'',''689'',''401'',''693'',''698'',''661'',''249'',''656'',''813'',''767'',''329'',''195'',''554'',''397'',''550'',''577'',''399'',''821'',''569'',''773'',''94'',''512'',''619'',''542'',''541'',''603'',''406'',''720'',''549'',''103'',''507'',''560'',''242'',''839'',''225'',''777'',''336'',''677'',''277'',''780'',''778'',''310'',''311'',''263'',''592'',''224'',''407'',''497'',''201'',''372'',''333'',''210'',''56'',''446'',''571'',''809'',''671'',''568'',''299'',''79'',''449'',''292'',''702'',''234'',''75'',''257'',''254'',''339'',''430'',''260'',''403'',''402'',''490'',''256'',''600'',''534'',''521'',''187'',''417'',''687'',''748'',''587'',''197'',''574'',''223'',''489'',''536'',''68'',''296'',''116'',''211'',''394'',''754'',''523'',''92'',''788'',''270'',''547'',''27'',''149'',''836'',''71'',''280'',''328'',''289'',''789'',''83'',''236'',''723'',''373'',''544'',''423'',''157'',''156'',''161'',''267'',''122'',''305'',''495'',''136'',''667'',''826'',''388'',''97'',''275'',''692'',''463'',''420'',''205'',''222'',''567'',''15'',''137'',''135'')',   \n" +
                "@List4Codes = '(''2013'')',   \n" +
                "@List5Codes = '',    \n" +
                "@List6Codes = '',     \n" +
                "@List7Codes = '',     \n" +
                "@NullValues = 0,     \n" +
                "@Thousand = '',     \n" +
                "@Decimal = '.',     \n" +
                "@DecPlaces = 2 ,   \n" +
                "@Limit = 0";

        Connection connection = null;
        try {
            connection = ds.getConnection();

            //CallableStatement statement = connection.prepareCall(ds.createQuery("QC"));
            CallableStatement statement = connection.prepareCall(query);
            ResultSet rs = statement.executeQuery();
            int columnsNumber = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i=1; i<=columnsNumber; i++)
                    System.out.print(rs.getObject(i) + ", ");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (connection!=null)
                try { connection.close(); } catch (Exception ex) { }
        }
    }

    private String createQuery(String domain) throws Exception {
        return "EXECUTE Warehouse.dbo.usp_GetDataTEST  \n" +
                "@DomainCode = 'QC',   \n" +
                "@lang = 'E',   \n" +
                "@List1Codes = '("+filteringParameter(domain,1)+")',   \n" + //country
                "@List2Codes = '("+filteringParameter(domain,2)+")',   \n" + //element
                "@List3Codes = '("+filteringParameter(domain,3)+")',   \n" + //crop
                "@List4Codes = '("+filteringParameter(domain,4)+")',   \n" + //year
                "@List5Codes = '',    \n" +
                "@List6Codes = '',     \n" +
                "@List7Codes = '',     \n" +
                "@NullValues = 0,     \n" +
                "@Thousand = '',     \n" +
                "@Decimal = '.',     \n" +
                "@DecPlaces = 2 ,   \n" +
                "@Limit = 0";
    }

    private String filteringParameter (String domain, int index) throws Exception {
        StringBuilder parameter = new StringBuilder();
        for (String code : filteringCodes(domain, index))
            parameter.append(",''").append(code).append("''");
        return parameter.length()>0 ? parameter.substring(1) : "";
    }

    private String[] filteringCodes(String domain, int index) throws Exception {
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse
        HttpURLConnection connection = (HttpURLConnection) new URL("http://faostat3.fao.org/wds/rest/procedures/usp_GetListBox/faostatdb/"+domain+'/'+index+"/1/E").openConnection();
        InputStream input = null;
        try {
            connection.setRequestMethod("GET");
            input = connection.getInputStream();
            Object[][] data = mapper.readValue(input, new TypeReference<Object[][]>() {});
            String[] codes = new String[data.length];
            for (int i=0; i<data.length; i++)
                codes[i] = (String)data[i][0];
            return codes;
        } finally {
            if (input!=null)
                try {input.close();} catch (Exception ex) {}
        }
    }
*/
}
