# CloudComputing.MapReduce
My MapReduce implementation for Cloud Computing course work at University of Reading


# Report
## Abstract
In this project I implemented a map-reduce concept from scratch. This concept is one of the core components of the Hadoop framework which is called MapReduce. This component is responsible for computation / data processing.
I implemented key concepts at the MapReduce component such as Job, TaskTracker, Map, Shuffle, and Reduce with a generic approach.
Each Map and Reduce class is an abstract generic class. I create spirited mapper and reducer classes for task one and two while inheriting from those base classes.
I also use C# and Task in dotnet for implementing concurrent computation needed for different phases of MapReduce. My simple hadoop engine has developed in a clean manner and follows object oriented principles as well.
For the first task we need only one map and reduce run, another other hand for the second task we need two sequential maps and reduce run with different criteria.

## MapReduce concept
# Job
A Job in the context of Hadoop MapReduce is the unit of work to be performed as requested by the client / user. The information associated with the Job includes the data to be processed (input data), MapReduce logic / program / algorithm, and any other relevant configuration information necessary to execute the Job.

In this project for creating a new job, I implemented a JobBuilder class. In this class we can set different configurations that we need for creating a new valid job. In the following code block you can see, how we can create a new job by JobBuilder:
After creating a new job instance with JobBuilder we need to run the created job. At the Run method I implemented the main behavior of MapReduce framework. Here we have different phases where Job will run them in a proper order and handle all the results and forward the result of the previous phase to the next phase until we reach the final phase and write the final result to a different store.

# Input
This is the input data/file to be processed. In this step, the sample file is input to MapReduce. We can use different types of input only by creating a new class which is inherited from FileInputFormat.
For example, I implement a simple FileInputFormat for text files. This TextInputFormat just reads each file line by line. We can implement another type of FileInputFormat such as a CSV reader.

# Split
In this step, Hadoop splits / divides our sample input file into different parts, each part made up of some line from the input file. Note that, for the purpose of this course work, we are considering one cpu core as a one split. However, this is not necessarily true in a real-time scenario.
By implementing an iterator pattern over input records, I splitted all the input records to the different MapContext and add each chunk of input records to one MapContext.

# Map
Map Task in MapReduce is performed using the Map() function. This part of the MapReduce is responsible for processing one or more chunks of data and producing the output results.
Here I created a new MapTask for each MapContext and processed each split according to the logic defined in the Mapper class. Each mapper works on each split at a time. Each mapper is treated as a task and multiple tasks are executed across different Tasks and coordinated by the TaskManager.

# Shuffle and Sort
In this step, outputs from all the mappers are shuffled, sorted to put them in order, and grouped before sending them to the next step. I created a new shuffling context for each CPU core which consists of a different number of Reduce Context. So, in the next phase, when we want to rescue the result of mapping we have an equal number of reduced concurrent tasks to the number of available cores on the system under the run.

# Reduce
This step is used to aggregate the outputs of mappers using the reduce() function. Output of the reducer is sent to the next and final step. Each reducer is treated as a task and multiple tasks are executed across different Tasks and coordinated by the TaskManager. Just like Mapper, each task processed each split according to the logic defined in the Reducer class.
