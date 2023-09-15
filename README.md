# CodeQuality
Sample application for code quality and UTC. This application will provide elementary examples on these topics

* ### Configuring test cases using PyTest
* ### Code analysis using MyPy
* ### Code formatting using Black 

* #### Pre-requisites 
  * We can install all the dependencies required using the below mentioned command
    * pip install -r requirements.txt

* ### Configuring test cases using PyTest
  * All the test cases has to be present in the $\color{green}{tests}$ directory.
  * For each file present in $\color{blue}{src}$ directory there will be corresponding file present in the $\color{green}{tests}$ directory starting with `test_{file_name}`.
  * Each file will contain test cases corresponding to every function starting with `test_{function_name}`.
  * In order to run the test case we can use following command => `python3 -m pytest --cov --cov-report=html:type-coverage_html_report --cov-fail-under=85`.
    * --cov-report=html:coverage_html_report => it defines in what format and directory report will be generated. Syntax => `--cov-report={format}:{directory_path}`.
    * --cov-fail-under=85 => Allows us to pass the code coverage criteria in percentage, if the percentage is not meet, then it will result in build failure.
    * The reports will be created in the directory path mentioned in above command.
    * Additional configurations can be mentioned in `.coveragerc` file. Such as packages to included/excluded for test coverage and many more.
  * For further details, Please visit https://docs.pytest.org/en/7.4.x/
* ### Code analysis using MyPy
  * In to order to run the analysis we need to run this command => `mypy --html-report type-coverage src/com/code/quality`
    * --html-report => it defines the format to the report 
    * `type-coverage` => it defines the directory path in which reports will be generated.
    * `src/com/code/quality` => path of the source code that need to be analyzed.
  * Additional configurations can be mentioned in `.mypy.ini` file. Such as what are the parameter to considered while analyzing the code.
  * For further details, Please visit https://mypy.readthedocs.io/en/stable/
* ### Code formatting using Black 
  * In order to format the file according to the PEP8 (https://pep8.org) standards. We can use $\color{red}{black}$ framework.
  * ##### Installation
    * Inorder to install black on your machine, we can use this command => pip install black.
    * We can also install the black plugin in IntelliJ as well, for smoother integration. The plugin name is `BlackConnect`.
    * The BlackConnect plugin provide configuration which allows to automatically format the file or format a file on a particular trigger. For further details, Please visit https://plugins.jetbrains.com/plugin/14321-blackconnect.
    * > Please make sure the black process is always running on your machine. Either you can run the process in background or we can `blackd` to run it manually everytime.
  * Format Single File <=> black sample_code.py
  * Format Multiple Files <=> black folder_name/
  * In Intellij we can format files via `Tools => BlackConnect => Reformat Whole File`
  * For further details, Please visit https://black.readthedocs.io/en/stable/.

