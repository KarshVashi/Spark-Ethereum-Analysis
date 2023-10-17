# Spark-Ethereum-Analysis

# Spark Ethereum Analysis

## Overview
This repository contains a comprehensive analysis of Ethereum transactions using Apache Spark. The project delves deep into various aspects of Ethereum transactions, from identifying high gas-consuming transactions to detecting potential scam activities and wash trading patterns.

## Features

- **Gas Guzzlers Analysis**: Identify and analyze Ethereum transactions that consume significant amounts of gas.
  
- **Scam Analysis**: A dedicated module to detect potential scam activities within Ethereum transactions by identifying suspicious patterns and behaviors.
  
- **Wash Trading Detection**: Uncover wash trading activities, where there's an artificial inflation of trading volumes by simultaneously buying and selling assets.

## Modules

1. **Gas Guzzlers (`gassguzzlers/gasguzzlers.py`)**: Focuses on transactions consuming high amounts of gas.
  
2. **Scam Analysis (`scam/scamsanalysis1.py`)**: Analyzes potential scam activities within Ethereum transactions.
  
3. **Wash Trading Detection (`washtrading/washtrading.py`)**: Identifies wash trading activities within Ethereum transactions.
  
4. **Part A Analysis (`part a/parta.py` & `part a2/parta2.py`)**: Initial steps in the Ethereum transaction analysis, setting up data structures or preliminary data processing.
  
5. **Part B Analysis (`part b/partb.py`)**: Continuation of the analysis series.
  
6. **Part C Analysis (`part c/partc.py`)**: Further steps in the Ethereum transaction analysis series.

## Setup & Usage

1. Clone the repository:
```bash
git clone https://github.com/KarshVashi/Spark-Ethereum-Analysis.git
```

2. Navigate to the project directory:
```bash
cd Spark-Ethereum-Analysis
```

3. Experiment and run the desired analysis script. For example, to run the Gas Guzzlers analysis:
```bash
python gassguzzlers/gasguzzlers.py
```


## Dependencies

- Apache Spark
- Python 3.x

## Contributing

Feel free to fork this repository, make changes, and submit pull requests. Any contributions, no matter how minor, are greatly appreciated.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

