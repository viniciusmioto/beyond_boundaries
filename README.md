# Beyond Boundaries: Research Methodology and Reproducibility Kit

## Overview
This repository contains the full pipeline used in the study *Beyond Boundaries: A Study of Research Output and Collaborative Linkages in Brazilian Computer Science*. The study examines collaboration patterns in Brazilian computer science research across different subfields using OpenAlex data spanning 2015-2024. It explores international collaboration networks, subfield-specific patterns, and the impact of interdisciplinary engagement.

This README provides a structured explanation of the repository, detailing the methodology, scripts, and notebooks used to generate results for the research paper. 

---

## 🔎 Research Questions
The study aims to answer the following questions:

- **RQ1:** To what extent are Brazilian subfields collaborating with researchers and institutions in other countries?
- **RQ2:** What are the key differences in collaboration networks across Brazilian subfields of Computer Science?

## 💻 Repository Structure

```
├── src/
│   ├── data_processing/
│   │   ├── 1_extract_publication_summary_of_countries.py
│   │   ├── 2_0_count_publications_per_subfield_country.py
│   │   ├── 2_1_concat_publications_per_subfield.py
│   │   ├── 2_2_compute_subfields_percentage.py
│   │   ├── 3_0_collect_publication_meta.py
│   │   ├── 3_1_concat_publication_meta.py
│   │   ├── 3_2_construct_collabnets_years.py
│   │   ├── 3_3_construct_collabnets_filter.py
│   │   ├── 4_0_combine_networks.py
│   │   ├── 4_1_calculate_international.py
│   │   ├── 5_0_compute_centralization.py
│   ├── visualization/
│   │   ├── 2_subfield_percentages.ipynb
│   │   ├── 4_international_heatmap.ipynb
│   │   ├── 5_betweenness_centralization.ipynb
│   │   ├── 6_degree_distribution.ipynb
│   │   ├── 7_gephi_legends.ipynb
│   └── utils/
│       ├── mappings.py
```

- **data_processing/**: Scripts for collecting, processing, and analyzing publication data.
- **visualization/**: Jupyter notebooks that generate figures and network visualizations.
- **utils/**: Utility scripts for standardizing and mapping data fields.

---

## 📁 Data Description

### `br_publications.csv`  
This CSV file contains metadata for academic publications. Below are its columns and their descriptions:

| Column Name           | Description                                                                                                      | Format/Example                                                                                  |
|-----------------------|------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `id`                  | Unique identifier for the publication.                                                                           | `W2928842276`                                                                                  |
| `doi`                 | Digital Object Identifier (DOI) of the publication.                                                             | `10.1016/j.patrec.2019.03.022`                                                                 |
| `title`               | Title of the publication.                                                                                        | `"A novel deep learning based framework for the detection and classification of breast cancer"` |
| `publication_year`    | Year the publication was released.                                                                               | `2019`                                                                                         |
| `authorships`         | List of authors in JSON format, including their IDs, names, institutions, and countries.                        | JSON string with nested fields (see example below).                                            |
| `subfield`            | Subfield of the publication in JSON format (e.g., `Artificial Intelligence`).                                   | `{"id": "subfields/1702", "display_name": "Artificial Intelligence"}`                          |
| `cited_by_count`      | Total number of citations the publication has received.                                                         | `685`                                                                                          |
| `counts_by_year`      | JSON object listing yearly citation counts for the past 7 years.                                                 | `{"6_year": 8, "5_year": 96, ..., "0_year": 26}`                                               |
| `primary_topic`       | Primary research topic of the publication (if available).                                                       | `{'id': 'T10862', 'display_name': 'AI in cancer detection'}`                                   |

#### Example `authorships` Entry:
```json
[
  {
    "id": "A5112426596",
    "name": "SanaUllah Khan",
    "institutions": [{"id": "I206573129", "display_name": "Islamia College University"}],
    "countries": ["PK"]
  }
]
```

## 🌐 Collaboration Network Structure

The script generates a **collaboration network** in GEXF format, where:  
- **Nodes** represent authors.  
- **Edges** represent collaborations between authors (co-authorship in a publication).  

### Node Attributes
| Attribute | Description                                                                 |
|-----------|-----------------------------------------------------------------------------|
| `label1`  | Country code of the author's primary institution (e.g., `PK`, `BR`, `US`). |
| `label2`  | Author's primary subfield (e.g., `Artificial Intelligence`).               |

### Edge Attributes
| Attribute | Description                                      |
|-----------|--------------------------------------------------|
| `weight`  | Number of times two authors collaborated.        |

### Example GEXF Snippet
```xml
<node id="A5076776322" label="A5076776322">
  <attvalues>
    <attvalue for="0" value="BR" />                 <!-- Country -->
    <attvalue for="1" value="Artificial Intelligence" />  <!-- Subfield -->
  </attvalues>
</node>
<edge source="A5076776322" target="A5112426596" weight="1" />
```


## Methodology

### 1. Data Collection
The study extracts publication metadata using OpenAlex’s API, focusing on Brazilian institutions in Computer Science from 2015-2024. The collected metadata includes:
- Authors and institutional affiliations
- Publication year
- Citation counts
- Subfields and topics

### 2. Data Processing
A sequence of scripts processes the raw data into meaningful analytical outputs:

#### 2.1 Extracting Country-Level Publication Data
- **`1_extract_publication_summary_of_countries.py`**: Summarizes publication counts per country.

#### 2.2 Computing Subfield Distributions
- **`2_0_count_publications_per_subfield_country.py`**: Counts publications per subfield per country.
- **`2_1_concat_publications_per_subfield.py`**: Merges results across subfields.
- **`2_2_compute_subfields_percentage.py`**: Normalizes subfield publication counts to compute percentages.

#### 2.3 Collecting and Aggregating Publication Metadata
- **`3_0_collect_publication_meta.py`**: Extracts detailed publication metadata.
- **`3_1_concat_publication_meta.py`**: Consolidates metadata files.

#### 2.4 Constructing Collaboration Networks
- **`3_2_construct_collabnets_years.py`**: Builds yearly collaboration networks based on co-authorship.
- **`3_3_construct_collabnets_filter.py`**: Filters networks based on collaboration strength.
- **`4_0_combine_networks.py`**: Merges networks over multiple years.

#### 2.5 Computing International Collaboration Metrics
- **`4_1_calculate_international.py`**: Computes international collaboration statistics.

#### 2.6 Computing Centralization Measures
- **`5_0_compute_centralization.py`**: Analyzes network centrality metrics (e.g., betweenness centrality).

### 3. Visualization
Several Jupyter notebooks are used to generate figures for the study:

- **`2_subfield_percentages.ipynb`**: Visualizes the share of each subfield in Brazilian CS research.
- **`4_international_heatmap.ipynb`**: Generates a heatmap of international collaboration.
- **`5_betweenness_centralization.ipynb`**: Analyzes centralization trends in collaboration networks.
- **`6_degree_distribution.ipynb`**: Plots degree distributions of collaboration networks.
- **`7_gephi_legends.ipynb`**: Provides legends and formatting for Gephi visualizations.

---

## Reproducibility Guide

To reproduce the study:

### 1. Set Up Environment
Install dependencies:
```bash
pip install -r requirements.txt
```

### 2. Run Data Processing Pipeline
Execute scripts in the following order:
```bash
python src/data_processing/1_extract_publication_summary_of_countries.py
python src/data_processing/2_0_count_publications_per_subfield_country.py
python src/data_processing/2_1_concat_publications_per_subfield.py
python src/data_processing/2_2_compute_subfields_percentage.py
python src/data_processing/3_0_collect_publication_meta.py
python src/data_processing/3_1_concat_publication_meta.py
python src/data_processing/3_2_construct_collabnets_years.py
python src/data_processing/3_3_construct_collabnets_filter.py
python src/data_processing/4_0_combine_networks.py
python src/data_processing/4_1_calculate_international.py
python src/data_processing/5_0_compute_centralization.py
```

### 3. Run Visualization Notebooks
Open Jupyter and run:
```bash
jupyter notebook
```
Then navigate to `src/visualization/` and execute the notebooks sequentially.

---

## Key Findings (from the paper)

- **International Collaboration:** Collaboration rates vary significantly across subfields, with AI and Networks showing higher international engagement, while Information Systems remains largely domestic.
- **Subfield Specialization:** Brazil shows a strong research focus on Information Systems, whereas fields like Computational Theory and Computer Vision have lower publication shares.
- **Network Centralization:** Collaboration networks exhibit small-world properties, with certain institutions and authors acting as major hubs in Brazilian research.

These findings contribute to a better understanding of Brazil’s integration into the global Computer Science research community and provide insights for shaping research policies and collaboration strategies.

---

## Conclusion
This repository provides a structured pipeline for analyzing scientific collaboration in Computer Science. By following this README, researchers can reproduce the study’s results and extend the methodology for future work.

For questions or issues, please refer to the repository’s GitHub page.


#### Linux/MAC
`export PYTHONPATH=$(pwd)/src:$PYTHONPATH`

#### Windows
`$env:PYTHONPATH = "$($pwd.Path)\src;$env:PYTHONPATH"`