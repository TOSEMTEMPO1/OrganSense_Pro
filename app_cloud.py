import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, date
import ast
import hashlib
import re
import random
import string
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import streamlit as st
import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine, text
from datetime import datetime, date
import ast, hashlib, re, random, string, requests, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- CONFIGURAÇÃO DO BANCO DE DADOS (POSTGRESQL CLOUD STARTUP) ---
@st.cache_resource
def get_engine():
    """Mantém a conexão persistente com o Supabase Cloud."""
    return create_engine(
        st.secrets["database"]["url"], 
        pool_pre_ping=True
    )

engine = get_engine()

def run_sql(query, params=None):
    """Executa queries SQL garantindo a integridade da transação laboratorial."""
    with engine.begin() as conn:
        return conn.execute(text(query), params or {})

# --- SCHEMA REBIRTH (ARQUITETURA INTEGRAL PARA IA E MACHINE LEARNING) ---
if "db_iniciado" not in st.session_state:
    with engine.begin() as conn:
        # 1. Tabela de Usuários e Perfis
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS usuarios_final (
                username TEXT PRIMARY KEY, 
                password TEXT, 
                email TEXT, 
                role TEXT
            )
        '''))
        # 2. Tabela de Pacientes (Dados Demográficos)
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS pacientes_final (
                id_p TEXT PRIMARY KEY, 
                nome TEXT, 
                idade INTEGER, 
                sexo TEXT, 
                nascimento TEXT, 
                estado TEXT, 
                city TEXT, 
                responsavel_cad TEXT, 
                hospital TEXT, 
                status_geral TEXT
            )
        '''))
        # 3. Tabela de Amostras (Dados Bioquímicos e Triagem)
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS amostras_final (
                id_s TEXT PRIMARY KEY, 
                id_p TEXT, 
                tumor_type TEXT, 
                localizacao TEXT, 
                estadiamento TEXT, 
                performance_status TEXT, 
                tnm TEXT, 
                tgo_ast TEXT, 
                tgp_alt TEXT, 
                bilirrubina TEXT, 
                creatinina TEXT, 
                ureia TEXT, 
                ischemia_time INTEGER, 
                transport TEXT, 
                q_score INTEGER, 
                responsavel TEXT
            )
        '''))
        # 4. Tabela de Histórico Clínico (Tratamentos Prévios)
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS clinico_final (
                id_p TEXT PRIMARY KEY, 
                tratado TEXT, 
                continua TEXT, 
                quimio TEXT, 
                radio TEXT, 
                cirurgia TEXT, 
                data_diag TEXT, 
                status_vital TEXT, 
                data_falecimento TEXT, 
                responsavel TEXT
            )
        '''))
        # 5. Tabela de Processamento de Organoides
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS organoides_final (
                id_o TEXT PRIMARY KEY, 
                id_s TEXT, 
                id_p TEXT, 
                responsavel TEXT, 
                data_inicio TEXT, 
                status TEXT, 
                reagentes TEXT, 
                observacoes TEXT, 
                intercorrencia TEXT, 
                passagem INTEGER, 
                n_ampolas INTEGER
            )
        '''))
        # 6. Tabela de Criopreservação (Estoque de Ampolas)
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS criopreservacao_final (
                id_unitario TEXT PRIMARY KEY, 
                id_o TEXT, 
                passagem INTEGER, 
                n_congelamento INTEGER, 
                densidade TEXT, 
                responsavel TEXT, 
                data TEXT
            )
        '''))
        # 7. Tabela de Log de Descongelamento
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS log_descongelamento_final (
                id_derivado TEXT PRIMARY KEY, 
                id_ampola_origem TEXT, 
                id_p TEXT, 
                viabilidade REAL, 
                destino TEXT, 
                responsavel TEXT, 
                data TEXT
            )
        '''))
        # 8. Tabela Fase 2: Plataforma de Ensaios Científicos (AI-Standard)
        # Inclui v_blank para rastreabilidade de fundo (Protocolo MTT)
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS ensaios_fase2_final (
                id_ensaio TEXT PRIMARY KEY, 
                id_unidade_derivada TEXT, 
                id_paciente TEXT, 
                categoria_ensaio TEXT, 
                tecnica TEXT, 
                grupo_exp TEXT, 
                placa_ref TEXT, 
                gene_alvo TEXT, 
                droga_teste TEXT, 
                dose TEXT, 
                tipo_replicata TEXT, 
                esquema_placa TEXT, 
                v1 TEXT, 
                c1 TEXT, 
                v2 TEXT, 
                c2 TEXT, 
                v3 TEXT, 
                c3 TEXT, 
                v_blank TEXT,
                responsavel TEXT, 
                data_registro TEXT
            )
        '''))
        # 9. Governança e Segurança
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY, 
                username TEXT, 
                acao TEXT, 
                detalhe TEXT, 
                data_hora TEXT
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS controle_chaves (
                mes_ano TEXT PRIMARY KEY, 
                chave_op TEXT, 
                chave_adm TEXT
            )
        '''))
    st.session_state.db_iniciado = True

# --- SISTEMAS DE AUDITORIA E E-MAIL ---
def log_audit(usuario, acao, detalhe):
    """Registra trilha de auditoria para conformidade institucional."""
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    run_sql(
        "INSERT INTO audit_logs (username, acao, detalhe, data_hora) VALUES (:u, :a, :d, :dh)", 
        {"u": usuario, "a": acao, "d": detalhe, "dh": agora}
    )

def disparar_email(destinatario, assunto, corpo, bcc_admin=False):
    """Envia notificações institucionais via SMTP."""
    try:
        remetente = st.secrets["email"]["remetente"]
        senha = st.secrets["email"]["senha_app"]
        msg = MIMEMultipart()
        msg['From'] = remetente
        msg['To'] = destinatario
        msg['Subject'] = assunto
        msg.attach(MIMEText(corpo, 'plain'))
        if bcc_admin: 
            msg['Bcc'] = remetente
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(remetente, senha)
        server.sendmail(remetente, [destinatario] + ([remetente] if bcc_admin else []), msg.as_string())
        server.quit()
        return True
    except:
        return False

# --- INTEGRAÇÃO LOCALIDADES (IBGE) ---
@st.cache_data(ttl=86400)
def get_ufs():
    """Busca lista de estados brasileiros em tempo real via API."""
    try:
        r = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/estados", timeout=5)
        return sorted([uf["sigla"] for uf in r.json()])
    except:
        return ["PE", "SP", "RJ", "MG", "Outro"]

@st.cache_data(ttl=86400)
def get_cidades(uf):
    """Busca municípios correspondentes à UF via API IBGE."""
    try:
        r = requests.get(f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios", timeout=5)
        return [c["nome"] for c in r.json()]
    except:
        return ["Recife", "Olinda", "Jaboatão", "Outra"]

# --- MAPEAMENTOS E SEGURANÇA ---
def make_hashes(p): return hashlib.sha256(str.encode(p)).hexdigest()
def check_hashes(p, h): return make_hashes(p) == h
def validar_senha(p): 
    """Exige padrão de alta segurança para senhas laboratoriais."""
    return len(p) >= 8 and re.search("[A-Z]", p) and re.search("[0-9]", p) and re.search("[_@$!%*#?&]", p)

# --- DICIONÁRIOS CIENTÍFICOS INTEGRALMENTE EXPANDIDOS ---
# --- DICIONÁRIOS CIENTÍFICOS EXPANDIDOS (MAPEAMENTO LITERATURA 100%) ---
PROTOCOLOS = {
    "PDAC": {
        "quimio": ["Gencitabina", "FOLFIRINOX", "nab-Paclitaxel + Gencitabina", "FOLFOX", "Capecitabina", "Oxaliplatina"], 
        "radio": ["SBRT (Radioterapia Estereotáxica)", "IMRT (Intensidade Modulada)", "Radioterapia Convencional 3D"], 
        "cirurgia": ["Whipple (Pancreatoduodenectomia)", "Pancreatectomia Distal", "Pancreatectomia Total", "Bypass Paliativo", "Biópsia de Diagnóstico"], 
        "drogas_anl": ["Gencitabina", "5-FU", "nab-Paclitaxel", "Oxaliplatina", "Irinotecano", "Derivado Tiazopiridínico (P1)"], 
        "genes_basais": ["KRAS", "CDKN2A", "SMAD4", "TP53"],
        "genes_quimio": ["hENT1", "RRM1", "RRM2", "ERCC1", "TYMS", "DPYD", "ABCG2"], # Transporte e Resistência 
        "genes_radio": ["HIF1A", "STAT3", "ATM"], # Hipóxia e Reparo
        "genes_cirurgia": ["TGFB1", "COL1A1", "ACTA2"], # Cicatrização e microambiente pós-op
        "genes_inicial": ["MUC1", "CEACAM1"], # Estádios I e II
        "genes_avancado": ["CD44", "Vimentina", "SNAIL", "TWIST", "ZEB1", "E-Cadherina"] # Estádios III e IV (EMT)
    },
    "GBM": {
        "quimio": ["Temozolomida (TMZ)", "Bevacizumabe", "Carmustina (BCNU)", "Lomustina (CCNU)"], 
        "radio": ["Holocraniana Convencional", "Estereotática Fracionada", "Radiocirurgia Estereotáxica"], 
        "cirurgia": ["Ressecção Total Macroscópica", "Ressecção Subtotal", "Biópsia Estereotáxica"], 
        "drogas_anl": ["Temozolomida", "Bevacizumabe", "Carmustina", "Derivado Tiazopiridínico (P1)"], 
        "genes_basais": ["EGFR", "PTEN", "TP53", "IDH1"],
        "genes_quimio": ["MGMT", "MSH6", "MLH1", "PMS2", "VEGF"], # Resistência TMZ/Beva
        "genes_radio": ["ATM", "ATR", "CHK1", "CHK2", "RAD51"], # Dano ao DNA
        "genes_cirurgia": ["HIF1A", "STAT3", "CHI3L1"], # Resposta a ferida/hipóxia cirúrgica
        "genes_proneural": ["PDGFRA", "OLIG2", "DLL3", "SOX2"], # Subtipo Proneural
        "genes_mesenquimal": ["CD44", "Vimentina", "SNAIL", "TWIST", "MET"], # Subtipo Mesenquimal
        "genes_classico": ["EGFR", "CDKN2A", "NES"] # Subtipo Clássico
    }
}
STATUS_MAP = {"Em Cultivo": "M", "Estabelecido": "E", "Inconclusivo": "I", "Contaminado": "C", "Manutenção": "M"}

# --- GERENCIAMENTO DE ACESSO ---
if 'logged_in' not in st.session_state: 
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("🔐 OrganSense Pro - Painel de Controle")
    tab_l, tab_r = st.tabs(["Acesso Restrito", "Novo Cadastro (Por Convite)"])
    
    with tab_l:
        u = st.text_input("Usuário")
        p = st.text_input("Senha", type="password")
        if st.button("Entrar no Sistema"):
            res = engine.connect().execute(text("SELECT password, role FROM usuarios_final WHERE username=:u"), {"u": u}).fetchone()
            if res and check_hashes(p, res[0]): 
                st.session_state.logged_in, st.session_state.username, st.session_state.role = True, u, res[1]
                log_audit(u, "Login", f"Acesso concedido (Perfil: {res[1]})")
                st.rerun()
            else: 
                st.error("Acesso Negado. Verifique usuário e senha.")
            
    with tab_r:
        st.subheader("🛡️ Trava de Segurança Institucional")
        mes_atual = datetime.now().strftime("%m/%Y")
        chave_db = engine.connect().execute(text("SELECT chave_op, chave_adm FROM controle_chaves WHERE mes_ano = :m"), {"m": mes_atual}).fetchone()
        
        if not chave_db:
            nova_chave_op = f"OP-{mes_atual[:2]}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=5))}"
            nova_chave_adm = f"ADM-{mes_atual[:2]}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=5))}"
            run_sql("INSERT INTO controle_chaves (mes_ano, chave_op, chave_adm) VALUES (:m, :cop, :cadm)", 
                    {"m": mes_atual, "cop": nova_chave_op, "cadm": nova_chave_adm})
            corpo_aviso = f"Administrador, as chaves para {mes_atual} foram geradas:\n\n🔑 OPERADOR: {nova_chave_op}\n👑 ADMINISTRADOR: {nova_chave_adm}"
            try: disparar_email(st.secrets["email"]["remetente"], f"Chaves de Segurança - {mes_atual}", corpo_aviso)
            except: pass
            c_op, c_adm = nova_chave_op, nova_chave_adm
        else:
            c_op, c_adm = chave_db[0], chave_db[1]

        chave_digitada = st.text_input("Chave Mestra de Autorização", type="password")
        if chave_digitada in [c_op, c_adm]:
            perfil_atribuido = "Administrador" if chave_digitada == c_adm else "Operador"
            st.success(f"Trava liberada! Um perfil de **{perfil_atribuido}** será criado.")
            nu = st.text_input("Novo Usuário", help="⚠️ REGRAS: Digite um nome de usuário sem espaços.")
            ne = st.text_input("E-mail Profissional")
            np = st.text_input("Senha Padrão Ouro", type="password", help="⚠️ REGRAS: Mínimo 8 caracteres, 1 Maiúscula, 1 Número e 1 Símbolo.")
            if st.button("Criar Perfil Profissional"):
                if validar_senha(np):
                    run_sql("INSERT INTO usuarios_final VALUES (:u,:p,:e,:r)", {"u":nu, "p":make_hashes(np), "e":ne, "r":perfil_atribuido})
                    st.success("Cadastro realizado com sucesso!")

else:
    st.set_page_config(page_title="OrganSense Pro Cloud", layout="wide")
    st.sidebar.markdown(f"👤 `{st.session_state.username}` | 🛡️ **{st.session_state.get('role', 'Operador')}**")
    if st.sidebar.button("Encerrar Sessão"): 
        log_audit(st.session_state.username, "Logout", "Saída")
        st.session_state.logged_in = False; st.rerun()
        
    opcoes_menu = ["Novo Cadastro", "Atualização de Status", "Relatório Consolidado"]
    if st.session_state.get("role") == "Administrador":
        opcoes_menu.append("⚙️ Painel de Governança")
        
    st.session_state.menu_opcao = st.sidebar.radio("Navegação", opcoes_menu)

    # --- TELA 1: NOVO CADASTRO (PADRONIZAÇÃO COMPLETA) ---
    if st.session_state.menu_opcao == "Novo Cadastro":
        st.header("📋 Cadastro de Novo Paciente")
        ufs = get_ufs()
        c1_geo, c2_geo = st.columns(2)
        uf_sel = c1_geo.selectbox("Estado (UF)", ufs, index=ufs.index("PE") if "PE" in ufs else 0)
        cid = c2_geo.selectbox("Cidade", get_cidades(uf_sel))
        
        with st.form("form_cad"):
            nome_p = st.text_input("Nome Completo", help="⚠️ REGRAS: Digite o nome completo sem abreviações.")
            c3, c4 = st.columns(2)
            sexo_p = c3.selectbox("Sexo", ["Masculino", "Feminino", "Outro"])
            data_nasc_p = c4.date_input("Data de Nascimento", value=date(1980, 1, 1), max_value=datetime.today())
            idade_calc = datetime.today().year - data_nasc_p.year
            st.info(f"Idade Calculada: **{idade_calc} anos**")
            hosp_f = st.text_input("Hospital de Origem", help="⚠️ REGRAS: Escreva o nome oficial completo.")
            
            if st.form_submit_button("Gerar Paciente"):
                count = engine.connect().execute(text("SELECT COUNT(*) FROM pacientes_final")).scalar()
                nid = f"OS-P{count + 1:03d}"
                run_sql("INSERT INTO pacientes_final VALUES (:id,:n,:i,:s,:na,:uf,:c,:rc,:h,'Apenas Cadastrado')", 
                        {"id":nid, "n":nome_p, "i":idade_calc, "s":sexo_p, "na":data_nasc_p.strftime("%d/%m/%Y"), "uf":uf_sel, "c":cid, "rc":st.session_state.username, "h":hosp_f})
                st.session_state.id_para_atualizar = nid; st.session_state.menu_opcao = "Atualização de Status"; st.rerun()

    # --- TELA 2: ATUALIZAÇÃO DE STATUS (ESTADOS ISOLADOS) ---
    elif st.session_state.menu_opcao == "Atualização de Status":
        id_b = st.text_input("Buscar ID do Paciente:", value=st.session_state.get('id_para_atualizar', '')).upper()
        if id_b:
            pac = engine.connect().execute(text("SELECT * FROM pacientes_final WHERE id_p = :id"), {"id": id_b}).fetchone()
            if pac:
                st.info(f"Paciente: {pac[1]} | Status Geral: {pac[9]}")
                fw_estados = ["Apenas Cadastrado", "Aquisição Realizada", "Em Processamento", "Fase Analítica"]
                if pac[9] != "Apenas Cadastrado":
                    if st.button("🔙 Desfazer e Voltar Fase Anterior"):
                        run_sql("UPDATE pacientes_final SET status_geral=:s WHERE id_p=:id", {"s": fw_estados[fw_estados.index(pac[9])-1], "id": id_b})
                        st.rerun()
                st.markdown("---")

                # SALA 1: ETAPA 1 (TRIAGEM)
                if pac[9] == "Apenas Cadastrado":
                    st.header("▶️ Etapa 1: Triagem e Histórico Clínico")
                    t_t = st.radio("Tipo de Tumor Base", ["PDAC (Pâncreas)", "GBM (Glioblastoma)"], horizontal=True)
                    sigla = "PDAC" if "PDAC" in t_t else "GBM"
                    n_am = st.number_input("Nº de Amostras Adquiridas", 1, 5)
                    l_am = []
                    for k in range(int(n_am)):
                        with st.expander(f"📦 Triagem Fisiológica da Amostra {k+1}", expanded=True):
                            c1, c2, c3 = st.columns(3)
                            if sigla=="PDAC":
                                loc, perf, stad = c1.selectbox("Origem", ["Primário", "Metastático"], key=f"loc{k}"), c2.selectbox("ECOG", ["0","1","2","3","4"], key=f"p{k}"), c3.selectbox("Estádio", ["I", "IIA", "IIB", "III", "IV"], key=f"s{k}")
                                t, n, m = c1.selectbox("T", ["T1","T2","T3","T4"], key=f"t{k}"), c2.selectbox("N", ["N0","N1","N2"], key=f"n{k}"), c3.selectbox("M", ["M0","M1"], key=f"m{k}")
                                tnm_f = f"{t}{n}{m}"
                            else:
                                loc, perf, stad = c1.selectbox("Origem", ["Primário", "Recorrente"], key=f"loc{k}"), c2.selectbox("KPS", ["100%","80%","50%","30%"], key=f"p{k}"), c3.selectbox("Subtipo", ["Clássico", "Proneural", "Mesenquimal"], key=f"s{k}")
                                idh, mg = c1.selectbox("Status IDH", ["WT", "Mut"], key=f"idh{k}"), c2.selectbox("MGMT", ["Metilado", "Não Metilado"], key=f"mg{k}")
                                tnm_f = f"{idh} / {mg}"
                            st.write("**🧬 Painel de Dano Bioquímico Sanguíneo**")
                            b = st.columns(5)
                            hlp_num = "⚠️ REGRAS: Insira apenas números e ponto (.)"
                            tg_a, tg_p, b_t, c_r, u_r = b[0].text_input("TGO", key=f"tg{k}", help=hlp_num), b[1].text_input("TGP", key=f"tp{k}", help=hlp_num), b[2].text_input("Bil.", key=f"bi{k}", help=hlp_num), b[3].text_input("Cre.", key=f"cr{k}", help=hlp_num), b[4].text_input("Ur.", key=f"ur{k}", help=hlp_num)
                            l_am.append((f"{id_b}-{sigla}-S{k+1:02d}", id_b, sigla, loc, stad, perf, tnm_f, tg_a, tg_p, b_t, c_r, u_r, 30, "Gelo", 100, st.session_state.username))
                    trt_check = st.radio("Tratamento oncológico prévio?", ["Não", "Sim"])
                    sv = {"q": "[]", "r": "[]", "c": "[]"}
                    if trt_check == "Sim":
                        t1, t2, t3 = st.tabs(["💊 Quimioterapia", "⚡ Radioterapia", "🔪 Cirurgia"])
                        with t1: sv["q"] = str(st.multiselect("Quimioterápicos Prévios:", PROTOCOLOS[sigla]["quimio"]))
                        with t2: sv["r"] = str(st.multiselect("Radioterapia Prévia:", PROTOCOLOS[sigla]["radio"]))
                        with t3: sv["c"] = str(st.multiselect("Procedimento Cirúrgico:", PROTOCOLOS[sigla]["cirurgia"]))
                    if st.button("Confirmar Etapa 1 e Avançar", type="primary"):
                        run_sql("DELETE FROM amostras_final WHERE id_p=:id", {"id": id_b})
                        for a in l_am: run_sql("INSERT INTO amostras_final VALUES (:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9,:v10,:v11,:v12,:v13,:v14,:v15,:v16)", {"v1":a[0],"v2":a[1],"v3":a[2],"v4":a[3],"v5":a[4],"v6":a[5],"v7":a[6],"v8":a[7],"v9":a[8],"v10":a[9],"v11":a[10],"v12":a[11],"v13":a[12],"v14":a[13],"v15":a[14],"v16":a[15]})
                        run_sql("INSERT INTO clinico_final VALUES (:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9,:v10)", {"v1":id_b,"v2":trt_check,"v3":"N/A","v4":sv["q"],"v5":sv["r"],"v6":sv["c"],"v7":"","v8":"Vivo","v9":"","v10":st.session_state.username})
                        run_sql("UPDATE pacientes_final SET status_geral='Aquisição Realizada' WHERE id_p=:id", {"id": id_b}); st.rerun()

                # SALA 2: PROCESSAMENTO
                elif pac[9] == "Aquisição Realizada":
                    st.header("🧪 Etapa 2: Início de Processamento (-O01-M)")
                    adb_list = engine.connect().execute(text("SELECT id_s FROM amostras_final WHERE id_p=:id"), {"id": id_b}).fetchall()
                    for aid in adb_list:
                        if not engine.connect().execute(text("SELECT id_o FROM organoides_final WHERE id_s=:ids"), {"ids": aid[0]}).fetchone():
                            if st.button(f"Iniciar Linhagem a partir de {aid[0]}", type="primary"):
                                run_sql("INSERT INTO organoides_final VALUES (:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9,:v10, :v11)", {"v1":f"{aid[0]}-O01-M", "v2":aid[0], "v3":id_b, "v4":st.session_state.username, "v5":datetime.now().strftime("%d/%m"), "v6":"Em Cultivo", "v7":"","v8":"","v9":"","v10":0, "v11":0})
                                run_sql("UPDATE pacientes_final SET status_geral='Em Processamento' WHERE id_p=:id", {"id":id_b}); st.rerun()

                # SALA 3: GESTÃO E CONGELAMENTO
                elif pac[9] == "Em Processamento":
                    st.header("⚙️ Gestão de Linhagens e Ampolas")
                    oat = engine.connect().execute(text("SELECT * FROM organoides_final WHERE id_p=:id"), {"id": id_b}).fetchall()
                    for o_row in oat:
                        with st.expander(f"Gerenciar ID: {o_row[0]}"):
                            nst = st.selectbox("Nova Situação Técnica", ["Em Cultivo", "Estabelecido", "Inconclusivo", "Contaminado"], key=f"st_{o_row[0]}")
                            ps_idx = st.number_input("Passagem Técnica", min_value=1, step=1, key=f"ps_{o_row[0]}")
                            aq, ad = 0, ""
                            if nst=="Estabelecido":
                                c_a1, c_a2 = st.columns(2)
                                aq = c_a1.number_input("Nº de ampolas", 1, key=f"aq_{o_row[0]}")
                                ad = c_a2.text_input("Densidade Celular", help="Ex: '1x10^6 cel/mL'", key=f"ad_{o_row[0]}")
                            ins, it = st.text_area("Insumos Adicionados", key=f"in_{o_row[0]}"), st.text_area("Intercorrência", key=f"it_{o_row[0]}")
                            if st.button("Atualizar Dossiê", key=f"bt_{o_row[0]}"):
                                nid_f = f"{o_row[0].rsplit('-', 1)[0]}-{STATUS_MAP[nst]}{ps_idx}"
                                run_sql("DELETE FROM organoides_final WHERE id_o=:o", {"o":o_row[0]})
                                run_sql("INSERT INTO organoides_final VALUES (:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9,:v10, :v11)", {"v1":nid_f,"v2":o_row[1],"v3":o_row[2],"v4":st.session_state.username,"v5":o_row[4],"v6":nst,"v7":ins,"v8":"","v9":it,"v10":ps_idx, "v11":aq})
                                if nst == "Estabelecido":
                                    for i in range(aq): run_sql("INSERT INTO criopreservacao_final VALUES (:v1,:v2,:v3,:v4,:v5,:v6,:v7)", {"v1":f"{nid_f}.{i+1}","v2":nid_f,"v3":ps_idx,"v4":i+1,"v5":ad,"v6":st.session_state.username,"v7":datetime.now().strftime("%d/%m")})
                                st.rerun()
                    v_amps_db = engine.connect().execute(text("SELECT id_unitario, densidade FROM criopreservacao_final WHERE id_unitario LIKE :p"), {"p": f"{id_b}%"}).fetchall()
                    if v_amps_db:
                        st.divider(); st.subheader("❄️ Log de Descongelamento")
                        dict_amp = {row[0]: row[1] for row in v_amps_db}
                        a_ori = st.selectbox("Escolha a Ampola", list(dict_amp.keys()))
                        viab = st.slider("Viabilidade encontrada (%)", 0, 100, 85)
                        dst = "Submetido à Análise" if viab >= 80 else "Manutenção"
                        if viab >= 80: st.success(f"🟢 Destino: Análise. Densidade: `{dict_amp[a_ori]}`")
                        else: st.error(f"🔴 Destino: Manutenção. Densidade: `{dict_amp[a_ori]}`")
                        if st.button("Gerar Unidade Derivada"):
                            c_d = len(engine.connect().execute(text("SELECT * FROM log_descongelamento_final WHERE id_ampola_origem=:id"), {"id": a_ori}).fetchall()) + 1
                            run_sql("INSERT INTO log_descongelamento_final VALUES (:v1,:v2,:v3,:v4,:v5,:v6,:v7)", {"v1":f"{a_ori}-{'A' if viab>=80 else 'M'}{c_d}", "v2":a_ori, "v3":id_b, "v4":viab, "v5":dst, "v6":st.session_state.username, "v7":datetime.now().strftime("%d/%m")})
                            st.rerun()
                    if st.button("✅ Avançar para a Fase Analítica", type="primary"):
                        run_sql("UPDATE pacientes_final SET status_geral='Fase Analítica' WHERE id_p=:id", {"id": id_b}); st.rerun()

                # --- SALA 4: FASE ANALÍTICA (RIGOR CIENTÍFICO) ---
                # --- CORREÇÃO DA LINHA 520 (Incompatibilidade de Coluna id_p) ---
                # --- BLOCO INTEGRAL E EXPANDIDO: FASE ANALÍTICA CIENTÍFICA ---
                # --- BLOCO INTEGRAL E EXPANDIDO: MOTOR DE INTELIGÊNCIA MOLECULAR ---
                # --- FASE ANALÍTICA (GERAÇÃO AUTOMÁTICA DE GENES POR VARIÁVEL CLÍNICA) ---
                # --- SALA 4: FASE ANALÍTICA (RIGOR CIENTÍFICO E MOTOR MOLECULAR) ---
                # --- BLOCO CORRIGIDO E DEFINITIVO: MOTOR DA FASE ANALÍTICA ---
                elif pac[9] == "Fase Analítica":
                    st.header("🧬 Fase 2: Plataforma de Ensaios Científicos (AI-Ready)")
                    
                    v_an = engine.connect().execute(text("SELECT id_derivado FROM log_descongelamento_final WHERE id_p=:id AND destino='Submetido à Análise'"), {"id": id_b}).fetchall()
                    
                    if v_an:
                        un_sel = st.selectbox("Unidade Analítica Ativa Selecionada (-Ax):", [x[0] for x in v_an])
                        col_c1, col_c2 = st.columns(2)
                        cat = col_c1.radio("Categoria Experimental:", ["Triagem Medicamentosa (Viabilidade)", "Caracterização Molecular (Gene/Proteína)"], horizontal=True)
                        
                        tecnica_sel = "MTT/PrestoBlue"
                        if "Caracterização" in cat:
                            tecnica_sel = col_c2.selectbox("Técnica de Análise:", ["rt-qPCR", "Western Blotting"])
                        
                        st.markdown("---")
                        # 1. Ajuste de Nomenclatura Exigido
                        grupo = st.selectbox("Grupo Experimental do Poço:", ["Controle Não-Tratado", "Controle (Veículo/Solvente)", "Tratado"])
                        
                        droga_final, dose_final = "N/A", "N/A"
                        tags_selecionadas = []
                        
                        # Recuperação de dados biológicos do paciente
                        am_ref = engine.connect().execute(text("SELECT tumor_type, estadiamento FROM amostras_final WHERE id_p=:id LIMIT 1"), {"id": id_b}).fetchone()
                        sigla = "PDAC" if am_ref and "PDAC" in am_ref[0] else "GBM"
                        h_cli = engine.connect().execute(text("SELECT tratado, quimio, radio, cirurgia FROM clinico_final WHERE id_p=:id"), {"id": id_b}).fetchone()
                        
                        if "tags_droga" not in st.session_state: st.session_state.tags_droga = []
                        if "tags_gene" not in st.session_state: st.session_state.tags_gene = []

                        # --- LÓGICA DE INTERFACE INTEGRADA (FIM DA REDUNDÂNCIA) ---
                        if grupo == "Controle Não-Tratado":
                            # Abre formulário direto, sem perguntar composto
                            if "Triagem" in cat:
                                tags_selecionadas = ["Controle Não-Tratado (Células + Meio)"]
                                droga_final = "N/A"
                                
                        elif grupo == "Controle (Veículo/Solvente)":
                            c_v1, c_v2 = st.columns(2)
                            droga_final = c_v1.selectbox("Tipo de Veículo:", ["DMSO", "PBS", "Etanol", "Outro"])
                            dose_final = c_v2.text_input("Titulação Final do Veículo:", placeholder="Ex: 0.1%")
                            if "Triagem" in cat:
                                # O próprio veículo selecionado engatilha o formulário
                                tags_selecionadas = [f"Veículo ({droga_final})"]

                        elif grupo == "Tratado":
                            c_t1, c_t2 = st.columns(2)
                            # O campo Fármaco agora É o multiselect
                            opcoes_droga = PROTOCOLOS[sigla]["drogas_anl"]
                            nova_droga = c_t2.text_input("➕ Novo Fármaco (Se não listado)")
                            if nova_droga and nova_droga not in st.session_state.tags_droga:
                                st.session_state.tags_droga.append(nova_droga)
                            
                            drogas_input = c_t1.multiselect("Fármaco / Tratamento Aplicado:", opcoes_droga + st.session_state.tags_droga)
                            dose_final = c_t2.text_input("Concentração / Dose Aplicada:", placeholder="Ex: 10µM")
                            
                            droga_final = ", ".join(drogas_input) if drogas_input else "N/A"
                            
                            if "Triagem" in cat:
                                # As drogas selecionadas abrem os formulários automaticamente
                                tags_selecionadas = drogas_input
                        
                        # --- GERAÇÃO AUTOMÁTICA DE GENES (APENAS PARA CARACTERIZAÇÃO) ---
                        if "Caracterização" in cat:
                            genes_auto = PROTOCOLOS[sigla]["genes_basais"].copy()
                            
                            if h_cli and h_cli[0] == "Sim":
                                if h_cli[1] != "[]": genes_auto.extend(PROTOCOLOS[sigla]["genes_quimio"])
                                if h_cli[2] != "[]": genes_auto.extend(PROTOCOLOS[sigla]["genes_radio"])
                                if h_cli[3] != "[]": genes_auto.extend(PROTOCOLOS[sigla].get("genes_cirurgia", []))
                            
                            if am_ref:
                                std = am_ref[1]
                                if sigla == "GBM":
                                    if std == "Proneural": genes_auto.extend(PROTOCOLOS[sigla].get("genes_proneural", []))
                                    elif std == "Mesenquimal": genes_auto.extend(PROTOCOLOS[sigla].get("genes_mesenquimal", []))
                                    elif std == "Clássico": genes_auto.extend(PROTOCOLOS[sigla].get("genes_classico", []))
                                elif sigla == "PDAC":
                                    if std in ["I", "IIA", "IIB"]: genes_auto.extend(PROTOCOLOS[sigla].get("genes_inicial", []))
                                    elif std in ["III", "IV", "Metastático"]: genes_auto.extend(PROTOCOLOS[sigla].get("genes_avancado", []))
                            
                            genes_auto = sorted(list(set(genes_auto)))
                            
                            st.write("**🏷️ Alvos Gênicos/Proteicos (Gerados Automáticamente Pela Literatura)**")
                            c_g1, c_g2 = st.columns([3, 1])
                            novo_gene = c_g2.text_input("➕ Adicionar Alvo Customizado")
                            if novo_gene and novo_gene not in st.session_state.tags_gene: 
                                st.session_state.tags_gene.append(novo_gene)
                                genes_auto.append(novo_gene)
                                
                            opcoes_totais = sorted(list(set(genes_auto + st.session_state.tags_gene)))
                            # Preenche o formulário de poços imediatamente com a injeção
                            tags_selecionadas = c_g1.multiselect("Alvos Analisados neste Grupo:", options=opcoes_totais, default=genes_auto)

                        # --- RENDERIZAÇÃO DOS FORMULÁRIOS DE POÇO (BRANCO E CELULAR OBRIGATÓRIOS) ---
                        if tags_selecionadas:
                            st.markdown("---")
                            n_r = st.radio("Esquema de Replicatas Técnicas:", ["Monoplicata", "Duplicata", "Triplicata"], horizontal=True)
                            placa_ref = st.text_input("Identificador do Experimento (Placa/Membrana/Lote):", value="Placa 01")
                            
                            coleta_dados = {}
                            for t in tags_selecionadas:
                                with st.container():
                                    st.markdown(f"#### 🧪 Leitura Bruta: `{t}`")
                                    if "Viabilidade" in cat: st.caption("Protocolo MTT: v1 = Absorbância Celular | b1 = Absorbância do Branco (SÓ MEIO).")
                                    elif tecnica_sel == "rt-qPCR": st.caption("Protocolo qPCR: v1 = Ct do Alvo | c1 = Ct do Endógeno (Ex: GAPDH).")
                                    
                                    p1, p2, p3 = st.columns(3)
                                    with p1:
                                        # "Celular" e "Branco" cobrados incondicionalmente em viabilidade
                                        v1 = st.text_input(f"Valor Celular P1 ({t})", key=f"v1_{t}")
                                        c1 = st.text_input(f"Controle/Endógeno P1 ({t})", key=f"c1_{t}") if "Caracterização" in cat else ""
                                        b1 = st.text_input(f"Branco P1 ({t})", key=f"b1_{t}") if "Triagem" in cat else ""
                                    
                                    v2, c2, b2, v3, c3, b3 = "", "", "", "", "", ""
                                    if n_r != "Monoplicata":
                                        with p2:
                                            v2 = st.text_input(f"Valor Celular P2 ({t})", key=f"v2_{t}")
                                            c2 = st.text_input(f"Controle P2 ({t})", key=f"c2_{t}") if "Caracterização" in cat else ""
                                            b2 = st.text_input(f"Branco P2 ({t})", key=f"b2_{t}") if "Triagem" in cat else ""
                                    if n_r == "Triplicata":
                                        with p3:
                                            v3 = st.text_input(f"Valor Celular P3 ({t})", key=f"v3_{t}")
                                            c3 = st.text_input(f"Controle P3 ({t})", key=f"c3_{t}") if "Caracterização" in cat else ""
                                            b3 = st.text_input(f"Branco P3 ({t})", key=f"b3_{t}") if "Triagem" in cat else ""
                                    
                                    coleta_dados[t] = {"v1":v1, "c1":c1, "v2":v2, "c2":c2, "v3":v3, "c3":c3, "blank": ",".join([x for x in [b1, b2, b3] if x != ""])}

                            if st.button("Registrar Dados no Dataset Mestre (TCGA-Ready)", type="primary"):
                                count_ens = engine.connect().execute(text("SELECT COUNT(*) FROM ensaios_fase2_final")).scalar()
                                for i, t in enumerate(tags_selecionadas):
                                    id_ens = f"ENS-{count_ens + i + 1:05d}"
                                    d = coleta_dados[t]
                                    
                                    # Lógica de Salvamento: Garante que droga_teste e gene_alvo fiquem corretos no banco
                                    nome_droga_banco = droga_final if "Caracterização" in cat else t
                                    nome_gene_banco = t if "Caracterização" in cat else "N/A"
                                    
                                    run_sql("INSERT INTO ensaios_fase2_final VALUES (:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9,:v10,:v11,:v12,:v13,:v14,:v15,:v16,:v17,:v18,:v19,:v20,:v21)",
                                            {"v1":id_ens, "v2":un_sel, "v3":id_b, "v4":cat, "v5":tecnica_sel, "v6":grupo, "v7":placa_ref, "v8":nome_gene_banco, "v9":nome_droga_banco, "v10":dose_final, "v11":"Técnica", "v12":n_r, "v13":d["v1"], "v14":d["c1"], "v15":d["v2"], "v16":d["c2"], "v17":d["v3"], "v18":d["c3"], "v19":d["blank"], "v20":st.session_state.username, "v21":datetime.now().strftime("%d/%m/%Y %H:%M:%S")})
                                log_audit(st.session_state.username, "Fase 2", f"Ensaio {tecnica_sel} ({grupo}) registrado para {id_b}")
                                st.success("Dados registrados com sucesso!"); st.rerun()
                                
    elif st.session_state.menu_opcao == "Relatório Consolidado":
        # Controle de estado para visualização individual
        if 'id_view_ativo' not in st.session_state:
            st.session_state.id_view_ativo = None

        # TELA A: LISTA GERAL E PESQUISA
        if not st.session_state.id_view_ativo:
            st.header("📊 Inteligência e Relatórios Consolidados")
            st.markdown("### Painel Gerencial de Pacientes")
            
            # Query para a tabela inicial solicitada
            query_lista = '''
                SELECT 
                    p.id_p as "ID", 
                    p.nome as "Nome", 
                    p.idade as "Idade", 
                    p.sexo as "Sexo", 
                    a.tumor_type as "Câncer",
                    p.status_geral as "Status"
                FROM pacientes_final p
                LEFT JOIN amostras_final a ON p.id_p = a.id_p
                GROUP BY p.id_p, p.nome, p.idade, p.sexo, a.tumor_type, p.status_geral
            '''
            df_lista = pd.read_sql(query_lista, engine)
            st.dataframe(df_lista, use_container_width=True, hide_index=True)
            
            st.divider()
            st.subheader("🔍 Acesso ao Dossiê Detalhado")
            id_busca = st.text_input("Digite o ID do Paciente (Ex: OS-P001):").upper()
            
            if st.button("Gerar Relatório de Rastreabilidade"):
                if id_busca:
                    # Validação de existência
                    res = engine.connect().execute(text("SELECT id_p FROM pacientes_final WHERE id_p=:id"), {"id": id_busca}).fetchone()
                    if res:
                        st.session_state.id_view_ativo = id_busca
                        st.rerun()
                    else:
                        st.error("ID não localizado na base de dados.")

        # TELA B: RELATÓRIO INDIVIDUAL (LIMPA A TELA)
        else:
            id_r = st.session_state.id_view_ativo
            if st.button("🔙 Voltar para Lista Geral"):
                st.session_state.id_view_ativo = None
                st.rerun()

            # Coleta de dados multidimensionais
            p_inf = engine.connect().execute(text("SELECT * FROM pacientes_final WHERE id_p=:id"), {"id": id_r}).fetchone()
            c_inf = engine.connect().execute(text("SELECT * FROM clinico_final WHERE id_p=:id"), {"id": id_r}).fetchone()
            a_df = pd.read_sql(f"SELECT * FROM amostras_final WHERE id_p='{id_r}'", engine)
            o_df = pd.read_sql(f"SELECT * FROM organoides_final WHERE id_p='{id_r}'", engine)
            e_df = pd.read_sql(f"SELECT * FROM ensaios_fase2_final WHERE id_paciente='{id_r}'", engine)

            st.title(f"📄 Dossiê Consolidado: {p_inf[1]} ({id_r})")
            st.info(f"**Status Operacional:** {p_inf[9]} | **Hospital de Origem:** {p_inf[8]}")

            st.subheader("⏳ Linha do Tempo de Rastreabilidade (Timeline)")

            # ETAPA 1: AQUISIÇÃO (DETECÇÃO DE PATOLOGIA)
            with st.expander("📌 1. Cadastro e Triagem Clínica/Bioquímica", expanded=True):
                c1, c2 = st.columns(2)
                c1.markdown(f"**Data de Cadastro:** {p_inf[4]}")
                c1.markdown(f"**Responsável Entrada:** `{p_inf[7]}`")
                c2.markdown(f"**Status Vital:** {c_inf[7] if c_inf else 'N/A'}")
                
                # Detecta o tipo de tumor para definir a visualização
                tipo_tumor = a_df['tumor_type'].iloc[0] if not a_df.empty else "Não Identificado"
                c2.markdown(f"**Diagnóstico Detectado:** `{tipo_tumor}`")
                
                if c_inf:
                    st.write("**Histórico de Tratamento Prévio:**")
                    st.write(f"- Quimio: {c_inf[3]} | Radio: {c_inf[4]} | Cirurgia: {c_inf[5]}")
                
                st.write("---")
                st.write(f"**📊 Detalhamento de Amostras ({tipo_tumor})**")
                
                # BIFURCAÇÃO DE EXIBIÇÃO: GBM vs PDAC
                if "GBM" in tipo_tumor:
                    # Colunas específicas para Glioblastoma
                    cols_gbm = ['id_s', 'localizacao', 'estadiamento', 'performance_status', 'tnm', 'responsavel']
                    df_exibir = a_df[cols_gbm].rename(columns={
                        'localizacao': 'Origem',
                        'estadiamento': 'Subtipo Histológico',
                        'performance_status': 'Escala KPS',
                        'tnm': 'Status IDH/MGMT'
                    })
                    st.dataframe(df_exibir, use_container_width=True, hide_index=True)
                
                elif "PDAC" in tipo_tumor:
                    # Colunas específicas para Pâncreas
                    cols_pdac = ['id_s', 'localizacao', 'estadiamento', 'performance_status', 'tnm', 'responsavel']
                    df_exibir = a_df[cols_pdac].rename(columns={
                        'localizacao': 'Origem',
                        'estadiamento': 'Estádio Clínico',
                        'performance_status': 'ECOG',
                        'tnm': 'TNM Final'
                    })
                    st.dataframe(df_exibir, use_container_width=True, hide_index=True)
                
                # Painel Bioquímico (Comum a ambos)
                st.write("**🩸 Painel de Dano Bioquímico Sanguíneo:**")
                st.table(a_df[['id_s', 'tgo_ast', 'tgp_alt', 'bilirrubina', 'creatinina', 'ureia']])

            # ETAPA 2: PROCESSAMENTO
            with st.expander("📌 2. Processamento e Evolução de Linhagens"):
                if not o_df.empty:
                    st.write("*Evolução cronológica in vitro:*")
                    st.dataframe(o_df[['id_o', 'status', 'passagem', 'data_inicio', 'responsavel']], use_container_width=True, hide_index=True)
                else:
                    st.warning("Sem registros de processamento celular para este ID.")

            # ETAPA 3: ANÁLISE CIENTÍFICA (MOTOR MOLECULAR RESTAURADO)
            # ETAPA 3: MOTOR ESTATÍSTICO DE BIOINFORMÁTICA (SUBSTITUI GRAPHPAD NA TRIAGEM)
            with st.expander("📌 3. Resultados Analíticos e Estatística (Fase 2)", expanded=True):
                if not e_df.empty:
                    st.write("**Dados Brutos Registrados:**")
                    st.dataframe(e_df[['id_ensaio', 'categoria_ensaio', 'tecnica', 'grupo_exp', 'gene_alvo', 'droga_teste', 'v1', 'v2', 'v3', 'c1', 'c2', 'c3']], hide_index=True)
                    
                    # Função interna de extração de arrays numéricos para a estatística
                    def extrair_replicatas(row, tecnica):
                        vals = []
                        try:
                            # Conversão segura para float
                            v = [pd.to_numeric(row[f'v{i}'], errors='coerce') for i in [1, 2, 3]]
                            c = [pd.to_numeric(row[f'c{i}'], errors='coerce') for i in [1, 2, 3]]
                            b_str = str(row['v_blank']).split(',')
                            b = [pd.to_numeric(b_str[i] if i < len(b_str) else 'nan', errors='coerce') for i in range(3)]
                            
                            for i in range(3):
                                if pd.isna(v[i]): continue
                                if tecnica == "MTT":
                                    if not pd.isna(b[i]): vals.append(v[i] - b[i])
                                elif tecnica == "rt-qPCR":
                                    if not pd.isna(c[i]): vals.append(v[i] - c[i]) # Delta Ct
                                elif tecnica == "Western Blotting":
                                    if not pd.isna(c[i]) and c[i] != 0: vals.append(v[i] / c[i]) # Razão DO Alvo/Carga
                        except: pass
                        return [x for x in vals if not pd.isna(x)]

                    def rodar_estatistica(ctrl_array, trt_array):
                        if len(ctrl_array) < 3 or len(trt_array) < 3:
                            return "N<3", "Inviável", "N/A", "ns"
                        
                        # Teste de Normalidade de Shapiro-Wilk
                        _, p_sw_ctrl = stats.shapiro(ctrl_array)
                        _, p_sw_trt = stats.shapiro(trt_array)
                        normal = (p_sw_ctrl > 0.05) and (p_sw_trt > 0.05)
                        
                        # Decisão do Teste
                        if normal:
                            stat_name = "T-test (Paramétrico)"
                            _, p_val = stats.ttest_ind(ctrl_array, trt_array)
                        else:
                            stat_name = "Mann-Whitney (Não-Paramétrico)"
                            _, p_val = stats.mannwhitneyu(ctrl_array, trt_array)
                        
                        # Atribuição de Significância
                        sig = "ns"
                        if p_val < 0.001: sig = "***"
                        elif p_val < 0.01: sig = "**"
                        elif p_val < 0.05: sig = "*"
                        
                        return "Passou" if normal else "Falhou", stat_name, f"{p_val:.4f}", sig

                    # --- FILTRO A: VIABILIDADE (MTT) ---
                    df_v = e_df[e_df['categoria_ensaio'].str.contains("Viabilidade", na=False)]
                    if not df_v.empty:
                        st.divider(); st.subheader("📊 Relatório de Viabilidade (MTT) - Análise Estatística")
                        
                        # Busca o grupo controle para normalizar em 100%
                        controle_v = df_v[df_v['grupo_exp'].str.contains("Controle")]
                        ctrl_vals_v = extrair_replicatas(controle_v.iloc[0], "MTT") if not controle_v.empty else []
                        media_ctrl_v = np.mean(ctrl_vals_v) if ctrl_vals_v else None

                        resultados_v = []
                        for _, row in df_v.iterrows():
                            trt_vals = extrair_replicatas(row, "MTT")
                            media_trt = np.mean(trt_vals) if trt_vals else 0
                            viab_pct = (media_trt / media_ctrl_v * 100) if media_ctrl_v and media_ctrl_v > 0 else 0
                            
                            if "Controle" in row['grupo_exp'] or not ctrl_vals_v:
                                norm, test, pval, sig = "N/A", "Referência (Controle)", "N/A", "N/A"
                            else:
                                norm, test, pval, sig = rodar_estatistica(ctrl_vals_v, trt_vals)
                            
                            resultados_v.append({
                                "Condição": row['droga_teste'],
                                "Dose": row['dose'],
                                "Viabilidade (%)": f"{viab_pct:.2f}%",
                                "Shapiro-Wilk (Normalidade)": norm,
                                "Teste Aplicado": test,
                                "P-Valor": pval,
                                "Significância": sig
                            })
                        st.dataframe(pd.DataFrame(resultados_v), hide_index=True)

                    # --- FILTRO B: CARACTERIZAÇÃO MOLECULAR (qPCR/WB) ---
                    df_m = e_df[e_df['categoria_ensaio'].str.contains("Caracterização", na=False)]
                    if not df_m.empty:
                        st.divider(); st.subheader("🧬 Painel Molecular (Expressão) - Análise Estatística")
                        
                        resultados_m = []
                        # Agrupa por gene para comparar Tratado vs Controle do MESMO gene
                        genes_unicos = df_m['gene_alvo'].unique()
                        
                        for gene in genes_unicos:
                            df_gene = df_m[df_m['gene_alvo'] == gene]
                            controle_m = df_gene[df_gene['grupo_exp'].str.contains("Controle")]
                            
                            tecnica = df_gene.iloc[0]['tecnica']
                            ctrl_vals_m = extrair_replicatas(controle_m.iloc[0], tecnica) if not controle_m.empty else []
                            
                            for _, row in df_gene.iterrows():
                                trt_vals = extrair_replicatas(row, tecnica)
                                valor_medio = np.mean(trt_vals) if trt_vals else 0
                                
                                if "Controle" in row['grupo_exp'] or not ctrl_vals_m:
                                    norm, test, pval, sig = "N/A", "Referência (Controle)", "N/A", "N/A"
                                else:
                                    norm, test, pval, sig = rodar_estatistica(ctrl_vals_m, trt_vals)
                                
                                # Ajuste visual do nome da métrica baseada na técnica
                                metrica_nome = "Média ΔCt" if tecnica == "rt-qPCR" else "Razão Média (DO)"
                                
                                resultados_m.append({
                                    "Gene/Alvo": gene,
                                    "Técnica": tecnica,
                                    "Condição": row['droga_teste'],
                                    metrica_nome: f"{valor_medio:.4f}",
                                    "Shapiro-Wilk": norm,
                                    "Teste Estatístico": test,
                                    "P-Valor": pval,
                                    "Significância": sig
                                })
                        st.dataframe(pd.DataFrame(resultados_m), hide_index=True)

                    # Download Unificado Bruto
                    csv = e_df.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Baixar Dataset Bruto (R/GraphPad)", csv, f"Analitico_Bruto_{id_r}.csv", "text/csv")
                else: 
                    st.warning("Sem dados analíticos registrados na Fase 2.")
                 
    elif st.session_state.menu_opcao == "⚙️ Painel de Governança":
        st.header("⚙️ Governança, Auditoria e Acessos")
        st.markdown("---")
        
        tab_log, tab_user = st.tabs(["📜 Logs de Auditoria", "👥 Usuários Ativos"])
        
        with tab_log:
            st.subheader("Rastreabilidade de Ações por Usuário")
            logs_df = pd.read_sql("SELECT data_hora as Data, username as Usuário, acao as Ação, detalhe as Detalhe FROM audit_logs ORDER BY id DESC LIMIT 200", engine)
            st.dataframe(logs_df, use_container_width=True, hide_index=True)
            
        with tab_user:
            st.subheader("Gestão de Perfis Institucionais")
            # CORREÇÃO: Aspas duplas em "E-mail" para evitar erro de sintaxe do PostgreSQL com o hífen (-)
            # FIX: Aspas duplas protegem o nome da coluna contra erros de sintaxe SQL
            usuarios_df = pd.read_sql('SELECT username as "Usuário", role as "Perfil", email as "E-mail" FROM usuarios_final', engine)
            st.table(usuarios_df)