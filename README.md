# buzzline-04-case

We can analyze and visualize different types of streaming data as the information arrives.

The producers don't change from buzzline-03-case - they write the same information to a Kafka topic, except the csv producer for the smart smoker has been modified to not run continuously. It will stop after reading all the rows in the CSV file.
The consumers have been enhanced to add visualization.

This project uses **matplotlib** and its animation capabilities for visualization.

It generates three applications:

1. A basic producer and consumer that exchange information via a dynamically updated file.
2. A JSON producer and consumer that exchange information via a Kafka topic.
3. A CSV producer and consumer that exchange information via a different Kafka topic.

All three applications produce live charts to illustrate the data.

## First, Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first.
**Python 3.11 is required.**

## Second, Copy This Example Project & Rename

1. Once the tools are installed, copy/fork this project into your GitHub account
   and create your own version of this project to run and experiment with.
2. Name it `buzzline-04-yourname` where yourname is something unique to you.

Additional information about our standard professional Python project workflow is available at
<https://github.com/denisecase/pro-analytics-01>.

Use your README.md to record your workflow and commands.

---

## Task 0. If Windows, Start WSL

Launch WSL. Open a PowerShell terminal in VS Code. Run the following command:

```powershell
wsl
```

You should now be in a Linux shell (prompt shows something like `username@DESKTOP:.../repo-name$`).

Do **all** steps related to starting Kafka in this WSL window.

---

## Task 1. Start Kafka (using WSL if Windows)

In P2, you downloaded, installed, configured a local Kafka service.
Before starting, run a short prep script to ensure Kafka has a persistent data directory and meta.properties set up. This step works on WSL, macOS, and Linux - be sure you have the $ prompt and you are in the root project folder.

1. Make sure the script is executable.
2. Run the shell script to set up Kafka.
3. Cd (change directory) to the kafka directory.
4. Start the Kafka server in the foreground.
5. Keep this terminal open - Kafka will run here
6. Watch for "started (kafka.server.KafkaServer)" message

```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

**Keep this terminal open!** Kafka is running and needs to stay active.

For detailed instructions, see [SETUP_KAFKA](https://github.com/denisecase/buzzline-02-case/blob/main/SETUP_KAFKA.md) from Project 2. 

---

## Task 2. Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment
2. Activate the virtual environment
3. Upgrade pip
4. Install from requirements.txt

### Windows

Open a new PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

Open a new terminal in VS Code (Terminal / New Terminal)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

---

## Task 3. Start a Basic (File-based, not Kafka) Streaming Application

This will take two terminals:

1. One to run the producer which writes to a file in the data folder.
2. Another to run the consumer which reads from the dynamically updated file.

### Producer Terminal

Start the producer to generate the messages.

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer.

Windows:

```shell
.venv\Scripts\activate
py -m producers.basic_json_producer_case
```

Mac/Linux:

```zsh
source .venv/bin/activate
python3 -m producers.basic_json_producer_case
```

### Consumer Terminal

Start the associated consumer that will process and visualize the messages.

In VS Code, open a NEW terminal in your root project folder.
Use the commands below to activate .venv, and start the consumer.

Windows:

```shell
.venv\Scripts\activate
py -m consumers.basic_json_consumer_case
```

Mac/Linux:

```zsh
source .venv/bin/activate
python3 -m consumers.basic_json_consumer_case
```

### Review the Application Code

Review the code for both the producer and the consumer.
Understand how the information is generated, written to a file, and read and processed.
Review the visualization code to see how the live chart is produced.
When done, remember to kill the associated terminals for the producer and consumer.

---

## Task 4. Start a (Kafka-based) JSON Streaming Application

This will take two terminals:

1. One to run the producer which writes to a Kafka topic.
2. Another to run the consumer which reads from that Kafka topic.

For each one, you will need to:

1. Open a new terminal.
2. Activate your .venv.
3. Know the command that works on your machine to execute python (e.g. py or python3).
4. Know how to use the -m (module flag to run your file as a module).
5. Know the full name of the module you want to run.
   - Look in the producers folder for json_producer_case.
   - Look in the consumers folder for json_consumer_case.

### Review the Application Code

Review the code for both the producer and the consumer.
Understand how the information is generated and written to a Kafka topic, and consumed from the topic and processed.
Review the visualization code to see how the live chart is produced.

Compare the non-Kafka JSON streaming application to the Kafka JSON streaming application.
By organizing code into reusable functions, which functions can be reused?
Which functions must be updated based on the sharing mechanism?
What new functions/features must be added to work with a Kafka-based streaming system?

When done, remember to kill the associated terminals for the producer and consumer.

---

## Task 5. Start a (Kafka-based) CSV Streaming Application

This will take two terminals:

1. One to run the producer which writes to a Kafka topic.
2. Another to run the consumer which reads from that Kafka topic.

For each one, you will need to:

1. Open a new terminal.
2. Activate your .venv.
3. Know the command that works on your machine to execute python (e.g. py or python3).
4. Know how to use the -m (module flag to run your file as a module).
5. Know the full name of the module you want to run.
   - Look in the producers folder for csv_producer_case.
   - Look in the consumers folder for csv_consumer_case.

### Review the Application Code

Review the code for both the producer and the consumer.
Understand how the information is generated and written to a Kafka topic, and consumed from the topic and processed.
Review the visualization code to see how the live chart is produced.

Compare the JSON application to the CSV streaming application.
By organizing code into reusable functions, which functions can be reused?
Which functions must be updated based on the type of data?
How does the visualization code get changed based on the type of data and type of chart used?
Which aspects are similar between the different types of data?

When done, remember to kill the associated terminals for the producer and consumer.

## Possible Explorations

- JSON: Process messages in batches of 5 messages.
- JSON:Limit the display to the top 3 authors.
- Modify chart appearance.
- Stream a different set of data and visualize the custom stream with an appropriate chart.
- How do we find out what types of charts are available?
- How do we find out what attributes and colors are available?

---

## How To Stop a Continuous Process

To kill the terminal, hit CTRL c (hold both CTRL key and c key down at the same time).

## Later Work Sessions

When resuming work on this project:

1. Open the project repository folder in VS Code. 
2. Start the Kafka service (use WSL if Windows) and keep the terminal running. 
3. Activate your local project virtual environment (.venv) in your OS-specific terminal.
4. Run `git pull` to get any changes made from the remote repo (on GitHub).

## After Making Useful Changes

1. Git add everything to source control (`git add .`)
2. Git commit with a -m message.
3. Git push to origin main.

```shell
git add .
git commit -m "your message in quotes"
git push -u origin main
```

## Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

## License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.

## Live Chart Examples

Live Bar Chart (JSON file streaming)

![Basic JSON (file-exchange)](images/live_bar_chart_basic_example.jpg)

Live Bar Chart (Kafka JSON streaming)

![JSON (Kafka)](images/live_bar_chart_example.jpg)

Live Line Chart with Alert (Kafka CSV streaming)

![CSV (Kafka)](images/live_line_chart_example.jpg)
