
```mermaid
graph LR;
    A-->B;
    A-->C;
    B-->C;
    C-->D;
    A[Máquina Local]
    B(S3)
    C(EC2)
    D[(Banco SQLite)]
```
