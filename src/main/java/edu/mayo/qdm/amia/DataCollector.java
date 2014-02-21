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

import java.util.*;

public class DataCollector {

    public static void main(String[] args) throws Exception {
        DataCollector test = new DataCollector();

        test.testLocal();
    }

    private List<RunResults> testLocal() throws Exception {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("qdm-grid-master-context.xml");
        ctx.registerShutdownHook();

        GridMaster gridMaster = ctx.getBean(GridMaster.class);

        Set<GridWorker> workers = new HashSet<GridWorker>();

        for(int i=0;i<3;i++){
            workers.add(
                    GridWorker.launch("localhost", Integer.parseInt("515" + Integer.toString(i)), "localhost", 1984, true));
        }

        List<RunResults> results = new ArrayList<RunResults>();
        for(int i=1;i<=20;i++){
            results.add(this.doTestLocal(1000 * i, gridMaster));
        }

        System.out.println("--------Local Data--------");
        for(RunResults result : results){
            System.out.println(result.patients + "," + result.time / 1000);
        }
        System.out.println("--------End Local Data--------");

        for(GridWorker worker : workers){
            worker.shutdown();
        }

        ctx.close();

        return results;
    }

    private RunResults doTestLocal(int multiple, GridMaster gridMaster) throws Exception {
        PatientIterable patients = new PatientIterable(multiple);

        String qdmXml = IOUtils.toString(new ClassPathResource("cypress/measures/ep/0033/hqmf1.xml").getInputStream());

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
