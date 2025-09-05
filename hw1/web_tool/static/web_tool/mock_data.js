// 這裡貼剛剛複製的 JSON
const mockData = {
  "columns": ["query_protein_name", "query_protein_length", "MME(query)_start", "MME(query)_end", "MME(query)", "k_mer"],
  "rows": [
    {
      "query_protein_name": "SPIKE_SARS2",
      "query_protein_length": 1273,
      "MME(query)_start": 1,
      "MME(query)_end": 6,
      "MME(query)": "MFVFLV",
      "k_mer": 6
    },
    {
      "query_protein_name": "SPIKE_SARS2",
      "query_protein_length": 1273,
      "MME(query)_start": 2,
      "MME(query)_end": 7,
      "MME(query)": "FVFLVG",
      "k_mer": 6
    }
    // ... 可以保留幾筆就好
  ],
  "count": 2
};
