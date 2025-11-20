# Projeto Base Python para EC2 com S3, Postgres, Pandas e AWS Secrets Manager

Este projeto é uma base para aplicações Python que:
- Roda em EC2
- Usa S3 para upload/download de arquivos
- Manipula arquivos CSV com Pandas
- Usa AWS Secrets Manager para variáveis de ambiente
- Salva dados em banco Postgres

## Estrutura de Pastas

```
├── .github/
│   └── copilot-instructions.md
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── s3_utils.py
│   ├── db_utils.py
│   └── main.py
├── scripts/
│   └── __init__.py
├── tests/
│   ├── __init__.py
│   └── test_sample.py
├── .env.example
├── requirements.txt
├── Dockerfile
├── Makefile
├── README.md
├── .flake8
└── pyproject.toml
```

## Como rodar localmente

1. Instale o Python 3.9+
2. Crie um virtualenv e ative:
   ```
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   source .venv/bin/activate  # Linux/Mac
   ```
3. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```
4. Configure as variáveis de ambiente conforme `.env.example`.
5. Execute:
   ```
   python src/main.py
   ```

## Como rodar na EC2

- Use o Dockerfile para buildar a imagem e rodar o container.
- Certifique-se de que a EC2 tenha permissões para acessar S3, Secrets Manager e o banco Postgres.

## Testes

```bash
pytest
```

## Lint e formatação

```bash
flake8 src/
black src/
```

---

> Substitua os placeholders conforme necessário para seu projeto.
