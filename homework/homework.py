"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import os
    from pathlib import Path
    import glob
    import zipfile
    import pandas as pd

    INPUT_DIR = Path("files/input")
    OUTPUT_DIR = Path("files/output")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    zip_paths = sorted(set(glob.glob(str(INPUT_DIR / "*.zip")) + glob.glob(str(INPUT_DIR / "*.csv.zip"))))

    if not zip_paths:
        raise SystemExit("No se encontraron archivos .zip o .csv.zip en files/input/")

    frames = []

    def leer_primer_csv_desde_zip(zip_path):
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_names = [name for name in z.namelist() if name.lower().endswith('.csv')]
            if not csv_names:
                raise ValueError(f"No se encontraron CSVs dentro de {zip_path}")
            with z.open(csv_names[0]) as fh:
                df = pd.read_csv(fh, low_memory=False)
        return df

    for zp in zip_paths:
        try:
            df = leer_primer_csv_desde_zip(zp)
            df['__source_file'] = Path(zp).name
            frames.append(df)
            print(f"Leído {zp} -> {df.shape[0]} filas, {df.shape[1]} columnas")
        except Exception as e:
            print(f"ERROR leyendo {zp}: {e}")

    if not frames:
        raise SystemExit("No se pudo leer ningún CSV desde los ZIPs.")

    df_all = pd.concat(frames, ignore_index=True, sort=False)
    print(f"Total combinado: {df_all.shape[0]} filas, {df_all.shape[1]} columnas")

    possible_id_cols = ['client_id', 'id', 'client', 'customer_id']
    found_id = None
    for c in possible_id_cols:
        if c in df_all.columns:
            found_id = c
            break

    if found_id is None:
        raise SystemExit("No se encontró columna de identificador de cliente (client_id) en los datos.")
    if found_id != 'client_id':
        df_all = df_all.rename(columns={found_id: 'client_id'})
        print(f"Renombrada columna {found_id} -> client_id")

    if 'mortage' in df_all.columns and 'mortgage' not in df_all.columns:
        df_all = df_all.rename(columns={'mortage': 'mortgage'})

    client_cols_required = [
        'client_id',
        'age',
        'job',
        'marital',
        'education',
        'credit_default',
        'mortgage'
    ]

    for col in client_cols_required:
        if col not in df_all.columns:
            df_all[col] = pd.NA
            print(f"Advertencia: no existía columna '{col}' en los datos de entrada; se creó vacía.")

    client_df = df_all[client_cols_required].copy()

    client_df['job'] = client_df['job'].astype("string").fillna(pd.NA)
    client_df['job'] = client_df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)

    client_df['marital'] = client_df['marital'].astype("string").fillna(pd.NA)

    client_df['education'] = client_df['education'].astype("string").fillna(pd.NA)
    client_df['education'] = client_df['education'].str.replace('.', '_', regex=False)
    client_df.loc[client_df['education'].str.lower() == 'unknown', 'education'] = pd.NA

    client_df['credit_default'] = client_df['credit_default'].astype("string").fillna('')
    client_df['credit_default'] = (client_df['credit_default'].str.lower() == 'yes').astype('Int64')

    client_df['mortgage'] = client_df['mortgage'].astype("string").fillna('')
    client_df['mortgage'] = (client_df['mortgage'].str.lower() == 'yes').astype('Int64')

    client_df['age'] = pd.to_numeric(client_df['age'], errors='coerce').astype('Int64')

    client_out = OUTPUT_DIR / "client.csv"
    client_df.to_csv(client_out, index=False)
    print(f"Guardado {client_out} ({client_df.shape[0]} filas, {client_df.shape[1]} columnas)")

    if 'campaign' in df_all.columns and 'number_contacts' not in df_all.columns:
        df_all = df_all.rename(columns={'campaign': 'number_contacts'})
    if 'contacts' in df_all.columns and 'number_contacts' not in df_all.columns:
        df_all = df_all.rename(columns={'contacts': 'number_contacts'})
    for alt in ['duration', 'call_duration', 'contact_time']:
        if alt in df_all.columns and 'contact_duration' not in df_all.columns:
            df_all = df_all.rename(columns={alt: 'contact_duration'})
            break

    if 'previous_campaign_contacts' not in df_all.columns:
        for alt in ['previous_contacts', 'previous_campaing_contacts', 'previous_campaign', 'pcontacts']:
            if alt in df_all.columns:
                df_all = df_all.rename(columns={alt: 'previous_campaign_contacts'})
                print(f"Renombrada {alt} -> previous_campaign_contacts")
                break

    if 'previous_outcome' not in df_all.columns and 'poutcome' in df_all.columns:
        df_all = df_all.rename(columns={'poutcome': 'previous_outcome'})

    if 'campaign_outcome' not in df_all.columns and 'y' in df_all.columns:
        df_all = df_all.rename(columns={'y': 'campaign_outcome'})

    for col in ['day', 'month']:
        if col not in df_all.columns:
            df_all[col] = pd.NA
            print(f"Advertencia: columna {col} no encontrada; creada vacía.")

    campaign_df = df_all[['client_id',
                        'number_contacts' if 'number_contacts' in df_all.columns else pd.NA,
                        'contact_duration' if 'contact_duration' in df_all.columns else pd.NA,
                        'previous_campaign_contacts' if 'previous_campaign_contacts' in df_all.columns else pd.NA,
                        'previous_outcome' if 'previous_outcome' in df_all.columns else pd.NA,
                        'campaign_outcome' if 'campaign_outcome' in df_all.columns else pd.NA,
                        'day',
                        'month']].copy()

    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].astype("string").fillna('')
    campaign_df['previous_outcome'] = (campaign_df['previous_outcome'].str.lower() == 'success').astype('Int64')

    campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].astype("string").fillna('')
    campaign_df['campaign_outcome'] = (campaign_df['campaign_outcome'].str.lower() == 'yes').astype('Int64')

    campaign_df['number_contacts'] = pd.to_numeric(campaign_df['number_contacts'], errors='coerce').astype('Int64')
    campaign_df['contact_duration'] = pd.to_numeric(campaign_df['contact_duration'], errors='coerce').astype('Int64')
    campaign_df['previous_campaign_contacts'] = pd.to_numeric(campaign_df['previous_campaign_contacts'], errors='coerce').astype('Int64')

    day_str = campaign_df['day'].astype("string").str.strip().replace('nan', '')
    month_str = campaign_df['month'].astype("string").str.strip().replace('nan', '')
    combined = (day_str + ' ' + month_str + ' 2022').str.strip()
    parsed_dates = pd.to_datetime(combined, dayfirst=True, errors='coerce')
    campaign_df['last_contact_date'] = parsed_dates.dt.strftime('%Y-%m-%d')

    campaign_final_cols = [
        'client_id',
        'number_contacts',
        'contact_duration',
        'previous_campaign_contacts',
        'previous_outcome',
        'campaign_outcome',
        'last_contact_date'
    ]
    campaign_out_df = campaign_df[campaign_final_cols].copy()

    campaign_out = OUTPUT_DIR / "campaign.csv"
    campaign_out_df.to_csv(campaign_out, index=False)
    print(f"Guardado {campaign_out} ({campaign_out_df.shape[0]} filas, {campaign_out_df.shape[1]} columnas)")

    invalid_dates = campaign_out_df['last_contact_date'].isna().sum()
    if invalid_dates:
        print(f"Advertencia: {invalid_dates} filas no pudieron parsear last_contact_date (se dejó vacío).")

    if 'const_price_idx' in df_all.columns and 'cons_price_idx' not in df_all.columns:
        df_all = df_all.rename(columns={'const_price_idx': 'cons_price_idx'})
    if 'eurobor_three_months' in df_all.columns and 'euribor_three_months' not in df_all.columns:
        df_all = df_all.rename(columns={'eurobor_three_months': 'euribor_three_months'})

    economics_cols_required = ['client_id', 'cons_price_idx', 'euribor_three_months']
    for col in economics_cols_required:
        if col not in df_all.columns:
            df_all[col] = pd.NA
            print(f"Advertencia: no existía '{col}' en datos de entrada; creado vacío.")

    economics_df = df_all[economics_cols_required].copy()
    economics_df['cons_price_idx'] = pd.to_numeric(economics_df['cons_price_idx'], errors='coerce')
    economics_df['euribor_three_months'] = pd.to_numeric(economics_df['euribor_three_months'], errors='coerce')

    economics_out = OUTPUT_DIR / "economics.csv"
    economics_df.to_csv(economics_out, index=False)
    print(f"Guardado {economics_out} ({economics_df.shape[0]} filas, {economics_df.shape[1]} columnas)")

    print("Proceso terminado.")

    return print("Proceso terminado.")

clean_campaign_data()