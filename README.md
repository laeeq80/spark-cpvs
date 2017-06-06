# Spark CPVS #

Spark-CPVS is a Spark-based library for setting up massively parallel Conformal Prediction Based Virtual Screening (CPVS) in Spark. It is based on earlier Spark-VS Pipelines project https://github.com/mcapuccini/spark-vs. In this project, we use conformal prediction, a mathematical method to filter out poor candidates and only screen ligands which are good leads. 

## Trying Out on Local
Clone the repo on local

On command line, perform the following command in the vs project directory

mvn clean install -DskipTests

This will install vs project in local maven repositories
	
Since OpenEye libraries are used under the hood, you need to own and a OpenEye license in order to run this. Therefore, you need to set a OE_LICENSE environment variable that points to the license, in your system to run the code in this repository.	

### Import vs.examples project in Scala IDE ###

1. File > Import > General > Existing project into workspace
2. Select spark-cpvs as root directory
3. Click finish
4. Wait for the workspace to build (this can take a while)
  If the IDE asks to include the scala library or compiler in the workspace click No

If you have scala version problems follow this procedure:

1. Right click on the project folder in the Package Explorer > Properties > Scala Compiler 
2. Select fixed scala installation 2.10.X
3. Click apply and let the IDE clean the project


Now you can get familiar with Spark-CPVS giving a look to the examples, and running them in Scala IDE. The main example is DockWithML. 
In the data directory you can find an example SDF as well as a receptor file. You will need a larger sdf file and relevant top scores. 
Remember that in order to run examples you need to specify arguments and OE_LICENSE environment variable through Run Configurations.
