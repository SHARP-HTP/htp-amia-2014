package edu.mayo.qdm.amia;

import com.google.common.collect.AbstractIterator;
import edu.mayo.qdm.cypress.CypressPatientDataSource;
import edu.mayo.qdm.executor.MeasurementPeriod;
import edu.mayo.qdm.executor.ResultCallback;
import edu.mayo.qdm.grid.master.GridMaster;
import edu.mayo.qdm.grid.worker.GridWorker;
import edu.mayo.qdm.patient.Patient;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

public class DataCollector {

    public static void main(String[] args) throws Exception {
        File outDir = new File("out");
        outDir.mkdirs();

        DataCollector test = new DataCollector();

        List<String> emeasures = Arrays.asList("0060", "0043", "0062", "0036", "0070", "0033");

        for(String emeasure : emeasures){
            File file = new File(outDir, emeasure + ".dat");
            file.createNewFile();
            FileOutputStream out = new FileOutputStream(file);

            for(RunResults result : test.testLocal(emeasure)){
                IOUtils.write(result.patients + "," + result.time / 1000, out);
            }

            out.close();
        }
    }

    private List<RunResults> testLocal(String emeasure) throws Exception {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("qdm-grid-master-context.xml");
        ctx.registerShutdownHook();

        GridMaster gridMaster = ctx.getBean(GridMaster.class);

        Set<GridWorker> workers = new HashSet<GridWorker>();

        for(int i=0;i<8;i++){
            workers.add(
                    GridWorker.launch("localhost", Integer.parseInt("515" + Integer.toString(i)), "localhost", 1984, true));
        }

        List<RunResults> results = new ArrayList<RunResults>();
        for(int i=1;i<=10;i++){
            results.add(this.doTestLocal(emeasure, 1000 * i, gridMaster));
        }

        for(GridWorker worker : workers){
            worker.shutdown();
        }

        ctx.close();

        return results;
    }

    private RunResults doTestLocal(String emasure, int multiple, GridMaster gridMaster) throws Exception {
        PatientIterable patients = new PatientIterable(multiple);

        String qdmXml = IOUtils.toString(new ClassPathResource("cypress/measures/ep/"+emasure+"/hqmf1.xml").getInputStream());

        final int[] total = {0};

        long startTime = System.currentTimeMillis();

        gridMaster.execute(patients, qdmXml, MeasurementPeriod.getCalendarYear(new DateTime(2012, 6, 1, 1, 1).toDate()), null, new ResultCallback() {
            @Override
            public void hit(String population, Patient patient) {
                total[0]++;
            }
        });

        System.out.println(total[0]);
        System.out.println("Total Time: " + (System.currentTimeMillis() - startTime));
        System.out.println("Total Patients: " + patients.counter);

        return new RunResults(patients.counter, (System.currentTimeMillis() - startTime), total[0]);
    }

    public static Iterator<Patient> multiply(final int size){
        return new AbstractIterator<Patient>() {

            CypressPatientDataSource cypressDataSource = new CypressPatientDataSource();

            int counter = 0;

            List<Patient> patients = (List<Patient>) cypressDataSource.getPatients();
            Iterator<Patient> cache = patients.iterator();

            @Override
            protected Patient computeNext() {
                if(counter > size){
                    return this.endOfData();
                }
                if(!cache.hasNext()){
                    Collections.shuffle(patients);
                    cache = patients.iterator();
                    counter++;
                }
                Patient p;
                try {
                    p = (Patient) cache.next().clone();
                } catch (CloneNotSupportedException e) {
                    throw new RuntimeException(e);
                }
                p.setSourcePid(UUID.randomUUID().toString());

                return p;
            }
        };
    }

    private static class PatientIterable implements Iterable<Patient>{

        private int counter = 0;
        private int multiple;

        PatientIterable(int multiple){
            super();
            this.multiple = multiple;
        }

        @Override
        public Iterator<Patient> iterator() {
            return new Iterator<Patient>() {
                Iterator<Patient> delegate = multiply(multiple);

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Patient next() {
                    counter++;
                    return delegate.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    };

    private static class RunResults {
        private int patients;
        private long time;
        private int hits;

        private RunResults(int patients, long time, int hits) {
            this.patients = patients;
            this.time = time;
            this.hits = hits;
        }

        @Override
        public String toString() {
            return "RunResults{" +
                    "patients=" + patients +
                    ", time=" + time +
                    ", hits=" + hits +
                    '}';
        }
    }
}
