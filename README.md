# data_neuron Installation Options

1. `pip install data_neuron` or `pip install data_neuron[]`:

   - Installs the base package
   - Includes SQLite support
   - Does NOT include PostgreSQL, MySQL, or MSSQL support

2. `pip install data_neuron[postgres]`:

   - Installs the base package
   - Includes SQLite support
   - Adds PostgreSQL support
   - Does NOT include MySQL or MSSQL support

3. `pip install data_neuron[mysql]`:

   - Installs the base package
   - Includes SQLite support
   - Adds MySQL support
   - Does NOT include PostgreSQL or MSSQL support

4. `pip install data_neuron[mssql]`:

   - Installs the base package
   - Includes SQLite support
   - Adds MSSQL support
   - Does NOT include PostgreSQL or MySQL support

5. `pip install data_neuron[all]`:
   - Installs the base package
   - Includes SQLite support
   - Adds PostgreSQL, MySQL, and MSSQL support

Note: SQLite support is always included as it's part of Python's standard library.
