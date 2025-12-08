package com.mycompany.swf_workload_generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class SWF_Workload_Generator
 * <p>
 * It creates correct and compatible workload trace for AleaNG simulator.
 * Especially, it correctly maps job user_ids, queue_ids and group_ids to user,
 * queue and group description files. The generator reads setup from
 * configuration.properties file.
 *
 * @author Dalibor Klusacek (klusacek@cesnet.cz)
 */
public class SWF_Workload_Generator {

    static ConfigurationReader cfg = null;
    static ArrayList<Job> created_jobs = new ArrayList();
    static String def_workload_filename = "default.swf";
    static String workload_filename = "";
    static String workload_dir = "";
    static String workload_id = "";
    static int workload_instances = 1;
    static int rnd_seed = 1024;
    static Output out = new Output();
    static String SWF_header = "; Version: 1.0\n"
            + "; Computer:  Synthetic\n"
            + "; Acknowledge: Dalibor Klusacek\n"
            + "; UnixStartTime: 1735689600\n"
            + "; --------------------------------------------------------------------------- \n"
            + "; JOB FIELDS MEANING: \n"
            + "; --------------------------------------------------------------------------- \n"
            + ";  0: job_id              (unique ID)\n"
            + ";  1: submit_time         (in seconds from 0)\n"
            + ";  2: wait_time           (in seconds)\n"
            + ";  3: runtime             (in seconds)\n"
            + ";  4: allocated_nodes     (number of all allocated nodes/or number of all allocated CPU cores)\n"
            + ";  5: CPU_time_used       (not used, same as runtime)\n"
            + ";  6: RAM                 (used, in kB)\n"
            + ";  7: required_nodes      (same as allocated nodes/cores)\n"
            + ";  8: walltime_limit      (user-provided upper bound runtime limit in seconds)\n"
            + ";  9: RAM                 (requested, in kB)\n"
            + "; 10: job_status          (always 1, not used) \n"
            + "; 11: user_id             (must match with user IDs specified in user desc. file)\n"
            + "; 12: group_id            (must match with group IDs specified in group desc. file)\n"
            + "; 13: executable_number   (-1, not used) \n"
            + "; 14: queue_id            (must match with queue IDs specified in queue desc. file)\n"
            + "; 15: partition           (-1, not used) \n"
            + "; 16: preceding_job       (-1, not used) \n"
            + "; 17: think_time          (-1, not used) \n"
            + "; 18: used_GPUs           (sum over all used nodes, must be divisible by the number of used nodes) \n"
            + "; 19: soft_walltime       (\"expected\" runtime in seconds - smaller than walltime - that can be exceeded without killing the job) \n"
            + "; 20: required_properties (separated by \":\", example: \"all:excl:2x4\" The first property \"all\" is used to match the String property of a cluster. If the property is \"all\", then this job can execute on any cluster. Otherwise, only a cluster that matches this String is acceptable. \"excl\" means that this job requires whole node(s) exclusively (no space-sharing with other jobs). \"2x4\" means that this job requires two nodes, each having at least 4 CPU cores.)\n"
            + "; --------------------------------------------------------------------------- \n";

    static String queues_header = "; id\tqueue_name\tCPU_quota\tpriority\n"
            + ";-----------------------------------------------------------------------------";
    static String machines_header = ";cl_id	name		nodes	CPUs	speed	GB_RAM	properties	GPUs\n"
            + ";-----------------------------------------------------------------------------";
    static String users_header = ";user_id	user_name	shares	CPU_quota\n"
            + ";-----------------------------------------------------------------------------";
    static String groups_header = ";group_id	group_name	shares	CPU_quota\n"
            + ";-----------------------------------------------------------------------------";

    //Setup of users, groups, queues and machines in the workload
    static int days = 0;
    static int total_cpus = 0;
    static double system_load = 0.0;
    static int load_percentage_urgent = 0;
    static int load_percentage_normal = 0;
    static int urgent_user_count = 0;
    static int normal_user_count = 0;
    static boolean allow_exclusive_jobs;
    static int exclusive_percentage = 0;
    static int largest_node_cpus = 0;

    static int batch_size = 0;
    static int batch_size_large = 1;
    static int batch_size_small = 1;
    static int batch_size_tiny = 1;

    static String[] user_names;
    static String[] user_names_urgent;
    static String[] user_names_normal;
    static String[] group_names;
    static String[] queue_names;
    static String[] machine_names;

    static boolean all_jobs_allocate_whole_nodes;
    static int[] number_of_nodes_for_job;
    static int[] tiny_job_sizes;
    static int[] small_job_sizes;
    static int[] large_job_sizes;
    static int[] job_runtime_limit;
    static int[] inter_batch_idle_period;
    static int[] inter_batch_idle_period_urgent;
    static int[] inter_batch_idle_period_normal;
    static int[] used_RAM = {1024 * 1024, 1024 * 1024 * 8, 1024 * 1024 * 16, 1024 * 1024 * 32};
    static int[] used_GPU_cards_per_node = {0, 0, 0, 0, 0, 1, 2, 2};

    public static void main(String[] args) {
        System.out.println("============================================");
        System.out.println("     Starting SWF WORKLOAD GENERATOR        ");
        System.out.println("============================================");
        System.out.println("");
        String user_dir = System.getProperty("user.dir");

        try {
            cfg = new ConfigurationReader();
        } catch (IOException e) {
            System.err.println("Could not load configuration file!" + e);
            return;
        }

        workload_instances = cfg.getInt("workload_instances");
        days = cfg.getInt("days");

        load_percentage_urgent = cfg.getInt("load_percentage_urgent");
        load_percentage_normal = 100 - load_percentage_urgent;
        allow_exclusive_jobs = cfg.getBoolean("allow_exclusive_jobs");
        exclusive_percentage = cfg.getInt("exclusive_percentage");
        total_cpus = cfg.getInt("total_cpus");
        largest_node_cpus = cfg.getInt("largest_node_cpus");
        system_load = cfg.getDouble("system_load");
        def_workload_filename = cfg.getString("workload_filename");
        workload_dir = cfg.getString("workload_dir");
        def_workload_filename = def_workload_filename.split("\\.")[0] + "-load_" + (Math.round(system_load * 100)) + "%-urgent_" + load_percentage_urgent + "%";

        user_names_urgent = cfg.getStringArray("user_names_urgent");
        user_names_normal = cfg.getStringArray("user_names_normal");
        urgent_user_count = user_names_urgent.length;
        normal_user_count = user_names_normal.length;

        group_names = cfg.getStringArray("group_names");
        queue_names = cfg.getStringArray("queue_names");
        machine_names = cfg.getStringArray("machine_names");

        batch_size_large = cfg.getInt("batch_size_large");
        batch_size_small = cfg.getInt("batch_size_small");
        batch_size_tiny = cfg.getInt("batch_size_tiny");

        number_of_nodes_for_job = cfg.getIntArray("number_of_nodes_for_job");
        tiny_job_sizes = cfg.getIntArray("tiny_job_sizes");
        small_job_sizes = cfg.getIntArray("small_job_sizes");
        large_job_sizes = cfg.getIntArray("large_job_sizes");

        job_runtime_limit = cfg.getIntArray("job_runtime_limit");
        inter_batch_idle_period_urgent = cfg.getIntArray("inter_batch_idle_period_urgent");
        inter_batch_idle_period_normal = cfg.getIntArray("inter_batch_idle_period_normal");

        all_jobs_allocate_whole_nodes = cfg.getBoolean("all_jobs_allocate_whole_nodes");

        for (int w = 0; w < workload_instances; w++) {
            // create new seed for each workload instance
            rnd_seed = rnd_seed + w;
            workload_id = "_" + w;
            //System.out.println(workload_filename);
            workload_filename = def_workload_filename.split("\\.")[0] + "-instance" + workload_id + ".swf";
            workload_filename = workload_dir + File.separator + workload_filename;
            Random seed = new Random(rnd_seed);
            Random excl_seed = new Random(rnd_seed);
            created_jobs.clear();
            generate_jobs("urgent", seed, excl_seed);
            generate_jobs("normal", seed, excl_seed);
            sort_and_print_jobs(user_dir);
            generate_queues(user_dir);
            generate_machines(user_dir);
            generate_users(user_dir);
            generate_groups(user_dir);
        }

        System.out.println("");
        System.out.println("=================================================");
        System.out.println(" Files written to: " + workload_dir + " directory");
        System.out.println("=================================================");

    }

    static void generate_jobs(String user_type, Random seed, Random excl_seed) {
        System.out.println("--------------------------------");
        System.out.println("Generating " + user_type + " jobs...");
        System.out.println("--------------------------------");

        //generate jobs in batches for all users
        int job_id = 1;
        int user_id_baseline = 0;
        int perc = 0;
        //max. cpu time for one day and desired system load
        long max_cpu_seconds_per_day = Math.round((total_cpus * 3600 * 24) * system_load);

        if (user_type.equals("urgent")) {
            user_names = user_names_urgent;
            perc = load_percentage_urgent;
            inter_batch_idle_period = inter_batch_idle_period_urgent;
            // how much these users can consume of the overal cput time
            max_cpu_seconds_per_day = Math.round(max_cpu_seconds_per_day * (perc / 100.0));
        } else {
            user_names = user_names_normal;
            perc = load_percentage_normal;
            inter_batch_idle_period = inter_batch_idle_period_normal;
            // to get correct user IDs in the merged file
            max_cpu_seconds_per_day = Math.round(max_cpu_seconds_per_day * (perc / 100.0));
            user_id_baseline = user_names_urgent.length;
        }

        // create all jobs for given user class
        long average_cpu_time_for_one_user = max_cpu_seconds_per_day / user_names.length;
        int current_user_id = 0;

        for (int i = 0; i < user_names.length; i++) {
            job_id = 1;
            current_user_id = i;
            System.out.println("~~~~~~~~ " + (user_names[current_user_id].split("\t")[0]) + " ~~~~~~~~");

            String user_name = user_names[current_user_id];
            int group = 0;
            int queue = 0;
            String properties = "";
            if (user_name.contains("urgent")) {
                properties = "all";
                group = 0;
                queue = 0;
            } else {
                properties = "normal";
                group = 1;
                // default low priority queue
                queue = Math.max(0,(queue_names.length-1));
            }
            int job_type = 0;
            if (user_name.contains("tiny")) {
                batch_size = batch_size_tiny;
                job_type = 0;
            } else if (user_name.contains("small")) {
                batch_size = batch_size_small;
                job_type = 1;
            } else {
                batch_size = batch_size_large;
                job_type = 2;
            }
            long current_time = 0;
            long start_time = 86400;
            // create all batches for a given user and all days
            for (int d = 0; d < days; d++) {
                System.out.println("---------- Day: " + d + "----------");
                //create batches for one day and one user
                current_time = (start_time * d);
                long cpu_time_for_one_user = 0;
                while (cpu_time_for_one_user < average_cpu_time_for_one_user) {

                    for (int b = 0; b < batch_size; b++) {

                        current_time += 1;
                        // if we move to next day, reset the job arrival timer
                        if (current_time > (start_time * d) + start_time) {
                            current_time = (start_time * d) + seed.nextInt(6 * 3600);
                            System.out.println("Arrival time reset...");
                        }
                        // choose walltime randomly from predefined "typical classes"
                        int walltime_limit = job_runtime_limit[seed.nextInt(job_runtime_limit.length)];
                        // choose runtime randomly from predefined walltime (and decrease it slightly by 0..2h) to reflect user walltime overestimation
                        int runtime = Math.max(60, walltime_limit - seed.nextInt(7200));
                        int ram = used_RAM[seed.nextInt(used_RAM.length)];

                        int required_CPUs_per_node = 1;

                        switch (job_type) {
                            case 0:
                                // urgent small user
                                required_CPUs_per_node = tiny_job_sizes[seed.nextInt(tiny_job_sizes.length)];
                                break;
                            case 1:
                                // urgent large user
                                required_CPUs_per_node = small_job_sizes[seed.nextInt(small_job_sizes.length)];
                                break;
                            case 2:
                                // normal tiny user
                                required_CPUs_per_node = large_job_sizes[seed.nextInt(large_job_sizes.length)];
                                break;

                            default:
                                throw new AssertionError();
                        }
                        int total_used_gpus = 0;
                        int number_of_nodes = 0;
                        if (all_jobs_allocate_whole_nodes) {
                            number_of_nodes = 1;
                            // since we ALLOCATE WHOLE NODES, here required_CPUs_per_node actually means the number of allocated whole nodes
                            total_used_gpus = used_GPU_cards_per_node[seed.nextInt(used_GPU_cards_per_node.length)] * required_CPUs_per_node;
                        } else {
                            number_of_nodes = number_of_nodes_for_job[seed.nextInt(number_of_nodes_for_job.length)];
                            total_used_gpus = used_GPU_cards_per_node[seed.nextInt(used_GPU_cards_per_node.length)] * number_of_nodes;
                        }

                        int soft_walltime = runtime;

                        // we provide additional description of job layout (num_nodes x CPUs per node)
                        String job_properties = properties;
                        if (allow_exclusive_jobs) {
                            // if the random number (0..100) falls bellow exclusive_percentage then this job is exclusive
                            if (exclusive_percentage > excl_seed.nextInt(0, 101)) {
                                job_properties = job_properties + ":excl";
                            }
                        }
                        if (!all_jobs_allocate_whole_nodes) {
                            job_properties = job_properties + ":" + number_of_nodes + "x" + required_CPUs_per_node;
                        }
                        
                        int queue_id = queue;

                        // if the job is exclusive, use whole node's size as the number of consumed cpu cores AND possibly use special queue for it
                        if (job_properties.contains(":excl")) {
                            cpu_time_for_one_user += runtime * largest_node_cpus * number_of_nodes;
                            int qindex = queuesContainThisQueue("exclusive");
                            System.out.println(job_id+" EXCL "+qindex+" prop:"+job_properties+" queue:"+queue);
                            if (qindex > -1 && job_properties.contains("normal")) {
                                queue_id = qindex;
                            }

                        } else {
                            cpu_time_for_one_user += runtime * required_CPUs_per_node * number_of_nodes;
                        }

                        if (cpu_time_for_one_user >= average_cpu_time_for_one_user) {
                            break;
                        }

                        if (job_id % 50 == 0) {
                            System.out.println(current_time + ": Adding " + job_id + " job from day " + d + " of user " + (user_names[current_user_id].split("\t")[0]) + " into workload... ");
                        }

                        String jobstring = "0 " + runtime + " " + (required_CPUs_per_node * number_of_nodes) + " " + runtime + " " + ram + " " + (required_CPUs_per_node * number_of_nodes)
                                + " " + walltime_limit + " " + ram + " 1 " + (current_user_id + user_id_baseline) + " " + group + " -1 " + queue_id + " -1 -1 -1 " + total_used_gpus + " " + soft_walltime + " " + job_properties;
                        Job j = new Job(current_time, job_id + "", jobstring);
                        created_jobs.add(j);

                        job_id++;

                    }
                    //go to next batch
                    current_time += Math.round(inter_batch_idle_period[seed.nextInt(inter_batch_idle_period.length)] * (1.0 / system_load));
                }
            }
            // move to next user and their batch of jobs

            System.out.println("User finished, jobs created = " + (job_id - 1));

        }
    }

    static void generate_queues(String user_dir) {
        System.out.println("--------------------");
        System.out.println("Generating queues...");
        System.out.println("--------------------");
        try {
            System.out.println("Deleting previous queues: " + user_dir + File.separator + workload_filename + ".queues");
            out.deleteResults(workload_filename + ".queues");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        try {
            System.out.println("Adding QUEUE header: \n" + queues_header);
            out.writeString(workload_filename + ".queues", queues_header);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        for (int i = 0; i < queue_names.length; i++) {
            try {
                System.out.println("Adding QUEUE: " + queue_names[i]);
                out.writeString(workload_filename + ".queues", i + "\t" + queue_names[i]);

            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }

    static void generate_machines(String user_dir) {
        System.out.println("----------------------");
        System.out.println("Generating machines...");
        System.out.println("----------------------");
        try {
            System.out.println("Deleting previous machines: " + user_dir + File.separator + workload_filename + ".machines");
            out.deleteResults(workload_filename + ".machines");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        try {
            System.out.println("Adding MACHINE header: \n" + machines_header);
            out.writeString(workload_filename + ".machines", machines_header);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        for (int i = 0; i < machine_names.length; i++) {
            try {
                System.out.println("Adding MACHINE: " + machine_names[i]);
                out.writeString(workload_filename + ".machines", i + "\t" + machine_names[i]);

            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }

    static void generate_groups(String user_dir) {
        System.out.println("--------------------");
        System.out.println("Generating groups...");
        System.out.println("--------------------");
        try {
            System.out.println("Deleting previous groups: " + user_dir + File.separator + workload_filename + ".groups");
            out.deleteResults(workload_filename + ".groups");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        try {
            System.out.println("Adding GROUP header: \n" + groups_header);
            out.writeString(workload_filename + ".groups", groups_header);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        for (int i = 0; i < group_names.length; i++) {
            try {
                System.out.println("Adding GROUP: " + group_names[i]);
                out.writeString(workload_filename + ".groups", i + "\t" + group_names[i]);

            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    static void generate_users(String user_dir) {
        System.out.println("-------------------");
        System.out.println("Generating users...");
        System.out.println("-------------------");
        try {
            System.out.println("Deleting previous users: " + user_dir + File.separator + workload_filename + ".users");
            out.deleteResults(workload_filename + ".users");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        try {
            System.out.println("Adding USER header: \n" + users_header);
            out.writeString(workload_filename + ".users", users_header);

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // add both user types, one by one
        int u_id = 0;
        for (int u = 0; u < 2; u++) {
            if (u == 0) {
                user_names = user_names_urgent;
            } else {
                user_names = user_names_normal;
            }
            for (int i = 0; i < user_names.length; i++) {
                try {
                    System.out.println("Adding USER: " + user_names[i]);
                    out.writeString(workload_filename + ".users", u_id + "\t" + user_names[i]);
                    u_id++;

                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    static void sort_and_print_jobs(String user_dir) {
        System.out.println("-------------------------");
        System.out.println("Merging generated jobs...");
        System.out.println("-------------------------");

        Collections.sort(created_jobs, new ArrivalComparator());

        try {
            System.out.println("Deleting previous workload: " + user_dir + File.separator + workload_filename);
            out.deleteResults(workload_filename);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        try {
            String full_SWF_header = SWF_header + "; number of jobs: " + created_jobs.size() + "\n;  ";
            System.out.println("Adding SWF workload header: " + user_dir + File.separator + workload_filename);
            out.writeString(workload_filename, full_SWF_header);

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        //generate jobs in batches for all users
        int job_id = 1;

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(workload_filename, true), 4 * 1024 * 1024)) { // 1 MB buffer

            PrintWriter pw = new PrintWriter(new FileWriter(workload_filename, true));

            // create all jobs
            for (int i = 0; i < created_jobs.size(); i++) {
                Job job = created_jobs.get(i);

                if (job_id % 100 == 0) {
                    System.out.println("Adding " + job_id + " into workload... ");
                }

                String jobstring = job_id + " " + job.getArrival() + " " + job.getJob();

                //pw.println(jobstring);
                bw.write(jobstring);
                bw.newLine();

                job_id++;
            }
            System.out.println("Workload generation completed. Total jobs created = " + (job_id - 1));
        } catch (IOException ex) {
            Logger.getLogger(SWF_Workload_Generator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static int queuesContainThisQueue(String queue) {

        for (int i = 0; i < queue_names.length; i++) {
            if (queue_names[i].contains(queue)) {
                //System.out.println(i+" Exclusive queue found: "+queue_names[i]);
                return i;
            }
        }
        return -1;
    }

}

class Job {

    private long arrival;
    private String id;
    private String job;

    public Job(long arrival, String id, String job) {
        setArrival(arrival);
        setId(id);
        setJob(job);
    }

    /**
     * @return the arrival
     */
    public long getArrival() {
        return arrival;
    }

    /**
     * @param arrival the arrival to set
     */
    public void setArrival(long arrival) {
        this.arrival = arrival;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the job
     */
    public String getJob() {
        return job;
    }

    /**
     * @param job the job to set
     */
    public void setJob(String job) {
        this.job = job;
    }

}
