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
    * The reports will be created in the directory path mentioned in above command.
    * Additional configurations can be mentioned in `.coveragerc` file. Such packages to included/excluded for test coverage and many more.
  * For further details, Please visit https://docs.pytest.org/en/7.4.x/
* #### Code analysis using MyPy
  * dsfsdf
  * For further details, Please visit https://mypy.readthedocs.io/en/stable/
* ### Code formatting using Black 
  * In order to format the file according to the PEP8 (https://pep8.org) standards. We can use $\color{red}{black}$ framework.
  * ##### Installation
    * Inorder to install black on your machine, we can use this command => pip install black.
    * We can also install the black plugin in IntelliJ as well, for smoother integration. The plugin name is `BlackConnect`.
    * The BlackConnect plugin provide configuration which allows to automatically format the file or format a file on a particular trigger. For further details, Please visit https://plugins.jetbrains.com/plugin/14321-blackconnect.
    * > Please make sure the black process is always running on your machine. Either you can run the process in background or we can `blackd` to run it manually everytime.
      >
  * Format Single File <=> black sample_code.py
  * Format Multiple Files <=> black folder_name/
  * In Intellij we can format files via `Tools => BlackConnect => Reformat Whole File`
  * For further details, Please visit https://black.readthedocs.io/en/stable/.

