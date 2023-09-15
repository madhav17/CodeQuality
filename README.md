# CodeQuality
Sample application for code quality and UTC. This application will provide elementary examples on these topics

* ### Configuring test cases using PyTest
* ### Code analysis using MyPy
* ### Code formatting using Black 

* #### Pre-requisites 
  * We can install all the dependencies required using the below mentioned command
    * pip install -r requirements.txt

* #### Configuring test cases using PyTest
  * All the test cases are present in the $\color{green}{tests}$ directory.
  * For each file present in $\color{blue}{src}$ directory there will be corresponding file present in the $\color{green}{tests}$ directory starting with `test_{file_name}`.
  * Each file will contain test cases corresponding to every function starting with `test_{function_name}`.
  * In order to run the test case we can use following command => `python3 -m pytest --cov --cov-report=html:coverage_html_report --cov-fail-under=85`.
    * --cov-report=html:coverage_html_report => it defines in what format and directory report will be generated. Syntax => `--cov-report={format}:{directory_path}`.

