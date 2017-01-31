def xml_quadro_operacoes(cnpj):

    def header_xml(dt_base, cnpj):

        query = 'select * from projeto_inv.xml_header where cnpjcpf="' + cnpj + '" and dtposicao=' + '"' + dt_base + '";'

        df = pd.read_sql(query, con=connection)
        if len(df) == 0:
            query = 'select * from projeto_inv.xml_header where cnpj="' + cnpj + '" and dtposicao=' + '"' + dt_base + '";'
            df = pd.read_sql(query, con=connection)
        df = df.sort(['cnpj', 'cnpjcpf', 'data_bd'], ascending=[True, True, False])
        df = df.drop_duplicates(subset=['cnpj', 'cnpjcpf'], take_last=False)
        df = df.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
        del df['index']
        global header_id_carteira
        header_id_carteira = df.get_value(0, 'header_id').astype(str)

    def tratamento_xml(dt_base, cnpjcpf):

        global quadro_oper
        quadro_oper = pd.DataFrame(
            columns=['mtm_info', 'produto', 'dt_emissao', 'dt_compra', 'a_p_op', 'quantidade', 'dt_vencto', 'indexador',
                     'perc_index', 'tx_operacao', 'principal', 'fundo', 'isin', 'cnpj', 'caracteristica', 'ativo',
                     'pu_mercado', 'pu_curva', 'pu_regra_xml', 'flag_desp'])

        query = 'select distinct * from projeto_inv.xml_header where cnpjcpf="' + cnpjcpf + '" and dtposicao=' + '"' + dt_base + '";'

        try:

            # selecionar a carga mais recente
            df = pd.read_sql(query, con=connection)
            if len(df) == 0:
                query = 'select * from projeto_inv.xml_header where cnpj=' + cnpjcpf + ' and dtposicao=' + '"' + dt_base + '";'
                df = pd.read_sql(query, con=connection)
            df = df.sort(['cnpj', 'cnpjcpf', 'data_bd'], ascending=[True, True, False])
            df = df.drop_duplicates(subset=['cnpj', 'cnpjcpf'], take_last=False)
            df = df.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
            del df['index']

            global header_id
            header_id = df.get_value(0, 'header_id').astype(str)
            patliq = df.get_value(0, 'patliq').astype(float)
            nome_fundo = df.get_value(0, 'nome')

        except:
            return None

            # debentures
        try:
            query = 'select isin, pu_mercado, pu_curva, pu_regra_xml, puposicao, dtemissao, dtoperacao, coddeb, classeoperacao, qtdisponivel, qtgarantia, dtvencimento, indexador, percindex, coupom, caracteristica, cnpjemissor , dtretorno,	puretorno,	indexadorcomp,	perindexcomp,	txoperacao,	classecomp from projeto_inv.xml_debenture a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_debenture where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            deb1 = pd.read_sql(query, con=connection)

            deb = deb1[deb1.dtretorno.isnull()].copy()

            deb['quantidade'] = deb['qtdisponivel'].astype(float) + deb['qtgarantia'].astype(float)
            deb['mtm_info'] = deb['puposicao'].astype(float) * deb['quantidade']
            deb['ativo'] = deb['coddeb']
            del deb['qtdisponivel']
            del deb['qtgarantia']
            del deb['coddeb']
            del deb['dtretorno']
            del deb['puretorno']
            del deb['indexadorcomp']
            del deb['perindexcomp']
            del deb['classecomp']
            del deb['txoperacao']
            deb['produto'] = 'debênture'
            deb = deb.rename(columns={'dtemissao': 'dt_emissao', 'dtoperacao': 'dt_compra', 'classeoperacao': 'a_p_op',
                                      'dtvencimento': 'dt_vencto', 'percindex': 'perc_index', 'coupom': 'tx_operacao',
                                      'cnpjemissor': 'cnpj', 'puposicao': 'pu'})
            deb['mtm_info'] = np.where(deb['a_p_op'] == 'V', -1 * deb['mtm_info'], deb['mtm_info'])
            deb = deb[~deb.mtm_info.isnull()]
            deb = deb[deb['a_p_op'] != 'T']

            if len(deb) > 0:
                quadro_oper = quadro_oper.append(deb)

        except:
            return None

            # debentures - em compromissada
        try:
            deb_comp = deb1[~deb1.dtretorno.isnull()].copy()
            deb_comp['classeoperacao'] = deb_comp['classecomp']
            del deb_comp['classecomp']
            deb_comp['qtgarantia1'] = np.where(deb_comp['classeoperacao'] == 'V', 0, deb_comp['qtgarantia'])
            deb_comp['quantidade'] = deb_comp['qtdisponivel'].astype(float) + deb_comp['qtgarantia1'].astype(float)
            deb_comp['mtm_info'] = deb_comp['puposicao'].astype(float) * deb_comp['quantidade']
            deb_comp['dtvencimento'] = deb_comp['dtretorno']
            del deb_comp['dtretorno']
            # deb_comp['puposicao']= deb_comp['puretorno']
            del deb_comp['puretorno']
            deb_comp['indexador'] = deb_comp['indexadorcomp']
            del deb_comp['indexadorcomp']
            deb_comp['percindex'] = deb_comp['perindexcomp']
            del deb_comp['perindexcomp']
            deb_comp['coupom'] = deb_comp['txoperacao']
            del deb_comp['txoperacao']
            deb_comp['ativo'] = deb_comp['coddeb']
            del deb_comp['qtdisponivel']
            del deb_comp['qtgarantia']
            del deb_comp['coddeb']
            deb_comp['produto'] = 'compromissada - debênture'
            deb_comp = deb_comp.rename(
                columns={'dtemissao': 'dt_emissao', 'dtoperacao': 'dt_compra', 'classeoperacao': 'a_p_op',
                         'dtvencimento': 'dt_vencto', 'percindex': 'perc_index', 'coupom': 'tx_operacao',
                         'cnpjemissor': 'cnpj', 'puposicao': 'pu'})
            #        deb_comp['mtm_info']=np.where(deb_comp['a_p_op']=='V',-1*deb_comp['mtm_info'],deb_comp['mtm_info'] )
            deb_comp = deb_comp[~deb_comp.mtm_info.isnull()]
            if len(deb_comp) > 0:
                quadro_oper = quadro_oper.append(deb_comp)

        except:
            return None

        # debêntures = tributos
        try:
            query = 'select sum(tributos) as tributos from projeto_inv.xml_debenture a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_debenture where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            deb_tributos = pd.read_sql(query, con=connection)
            deb_tributos['mtm_info'] = -1 * deb_tributos['tributos']
            del deb_tributos['tributos']
            deb_tributos['produto'] = 'tributos'
            deb_tributos = deb_tributos[~deb_tributos.mtm_info.isnull()]
            if len(deb_tributos) > 0:
                quadro_oper = quadro_oper.append(deb_tributos)

        except:
            return None

        # titprivado
        try:

            query = 'select isin,  pu_mercado, pu_curva, pu_regra_xml, puposicao, dtemissao, dtoperacao, codativo,	classeoperacao, qtdisponivel, qtgarantia, dtvencimento, indexador, percindex, coupom, caracteristica, cnpjemissor from projeto_inv.xml_titprivado a right join (select header_id,max(data_bd) as data_bd1 from projeto_inv.xml_titprivado where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            titprivado = pd.read_sql(query, con=connection)

            titprivado['quantidade'] = titprivado['qtdisponivel'].astype(float) + titprivado['qtgarantia'].astype(float)
            titprivado['mtm_info'] = titprivado['puposicao'].astype(float) * titprivado['quantidade']
            titprivado['ativo'] = titprivado['codativo']
            del titprivado['qtdisponivel']
            del titprivado['qtgarantia']
            del titprivado['codativo']
            titprivado['produto'] = 'título privado'
            titprivado = titprivado.rename(
                columns={'dtemissao': 'dt_emissao', 'dtoperacao': 'dt_compra', 'classeoperacao': 'a_p_op',
                         'dtvencimento': 'dt_vencto', 'percindex': 'perc_index', 'coupom': 'tx_operacao',
                         'cnpjemissor': 'cnpj', 'puposicao': 'pu'})
            titprivado['mtm_info'] = np.where(titprivado['a_p_op'] == 'V', -1 * titprivado['mtm_info'],
                                              titprivado['mtm_info'])
            titprivado = titprivado[~titprivado.mtm_info.isnull()]
            titprivado = titprivado[titprivado['a_p_op'] != 'T']
            if len(titprivado) > 0:
                quadro_oper = quadro_oper.append(titprivado)

        except:
            return None

        # titprivado: tributos
        try:

            query = 'select sum(tributos) as tributos from projeto_inv.xml_titprivado a right join (select header_id,max(data_bd) as data_bd1 from projeto_inv.xml_titprivado where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            titprivado_tributos = pd.read_sql(query, con=connection)
            titprivado_tributos['mtm_info'] = -1 * titprivado_tributos['tributos']
            del titprivado_tributos['tributos']
            titprivado_tributos['produto'] = 'tributos'
            titprivado_tributos = titprivado_tributos[~titprivado_tributos.mtm_info.isnull()]
            if len(titprivado_tributos) > 0:
                quadro_oper = quadro_oper.append(titprivado_tributos)

        except:
            return None

        try:
            # titpublico

            query = 'select isin, pu_mercado, pu_curva, pu_regra_xml, puposicao, dtemissao, dtoperacao, codativo,	classeoperacao, qtdisponivel, qtgarantia, dtvencimento, indexador, percindex, caracteristica, coupom, dtretorno,	puretorno,	indexadorcomp,	perindexcomp,	txoperacao,	classecomp from projeto_inv.xml_titpublico a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_titpublico where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            titpublico1 = pd.read_sql(query, con=connection)

            titpublico = titpublico1[titpublico1.dtretorno.isnull()].copy()

            titpublico['quantidade'] = titpublico['qtdisponivel'] + titpublico['qtgarantia']
            titpublico['mtm_info'] = titpublico['puposicao'] * titpublico['quantidade']
            titpublico['ativo'] = titpublico['codativo']
            del titpublico['qtdisponivel']
            del titpublico['qtgarantia']
            del titpublico['codativo']
            del titpublico['dtretorno']
            del titpublico['puretorno']
            del titpublico['indexadorcomp']
            del titpublico['perindexcomp']
            del titpublico['classecomp']
            del titpublico['txoperacao']
            titpublico['produto'] = 'titulo público'
            titpublico = titpublico.rename(
                columns={'dtemissao': 'dt_emissao', 'dtoperacao': 'dt_compra', 'classeoperacao': 'a_p_op',
                         'dtvencimento': 'dt_vencto', 'percindex': 'perc_index', 'coupom': 'tx_operacao',
                         'cnpjemissor': 'cnpj', 'puposicao': 'pu'})
            titpublico['mtm_info'] = np.where(titpublico['a_p_op'] == 'V', -1 * titpublico['mtm_info'],
                                              titpublico['mtm_info'])
            titpublico = titpublico[~titpublico.mtm_info.isnull()]
            titpublico = titpublico[titpublico['a_p_op'] != 'T']

            if len(titpublico) > 0:
                quadro_oper = quadro_oper.append(titpublico)

        except:
            return None

        try:
            # compromissada de titpublico

            titpublico_comp = titpublico1[~titpublico1.dtretorno.isnull()].copy()
            titpublico_comp['classeoperacao'] = titpublico_comp['classecomp']
            del titpublico_comp['classecomp']
            titpublico_comp['qtgarantia1'] = np.where(titpublico_comp['classeoperacao'] == 'V', 0,
                                                      titpublico_comp['qtgarantia'])
            titpublico_comp['quantidade'] = titpublico_comp['qtdisponivel'] + titpublico_comp['qtgarantia1']
            titpublico_comp['mtm_info'] = titpublico_comp['puposicao'] * titpublico_comp['quantidade']
            titpublico_comp['ativo'] = titpublico_comp['codativo']
            del titpublico_comp['qtdisponivel']
            del titpublico_comp['qtgarantia']
            del titpublico_comp['codativo']
            titpublico_comp['dtvencimento'] = titpublico_comp['dtretorno']
            del titpublico_comp['dtretorno']
            del titpublico_comp['puretorno']
            titpublico_comp['indexador'] = titpublico_comp['indexadorcomp']
            del titpublico_comp['indexadorcomp']
            titpublico_comp['percindex'] = titpublico_comp['perindexcomp']
            del titpublico_comp['perindexcomp']
            titpublico_comp['coupom'] = titpublico_comp['txoperacao']
            del titpublico_comp['txoperacao']

            titpublico_comp['produto'] = 'compromissada: titulo público'
            titpublico_comp = titpublico_comp.rename(
                columns={'dtemissao': 'dt_emissao', 'dtoperacao': 'dt_compra', 'classeoperacao': 'a_p_op',
                         'dtvencimento': 'dt_vencto', 'percindex': 'perc_index', 'coupom': 'tx_operacao',
                         'cnpjemissor': 'cnpj', 'puposicao': 'pu'})
            #        titpublico_comp['mtm_info']=np.where(titpublico_comp['a_p_op']=='V',-1*titpublico_comp['mtm_info'],titpublico_comp['mtm_info'])
            titpublico_comp = titpublico_comp[~titpublico_comp.mtm_info.isnull()]
            if len(titpublico_comp) > 0:
                quadro_oper = quadro_oper.append(titpublico_comp)


        except:
            return None


            # titpublico-tributos
        try:

            query = 'select sum(tributos) as tributos from projeto_inv.xml_titpublico a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_titpublico where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            titpublico_tributos = pd.read_sql(query, con=connection)
            titpublico_tributos['mtm_info'] = -1 * titpublico_tributos['tributos']
            del titpublico_tributos['tributos']
            titpublico_tributos['produto'] = 'tributos'
            titpublico_tributos = titpublico_tributos[~titpublico_tributos.mtm_info.isnull()]
            if len(titpublico_tributos) > 0:
                quadro_oper = quadro_oper.append(titpublico_tributos)

        except:
            return None

        try:
            # acoes

            query = 'select valorfindisp, classeoperacao, qtdisponivel, qtgarantia, codativo, valorfinemgar, isin, puposicao, pu_regra_xml from projeto_inv.xml_acoes a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_acoes where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            acoes = pd.read_sql(query, con=connection)
            acoes['mtm_info'] = acoes['valorfindisp'].astype(float) + acoes['valorfinemgar'].astype(float)
            acoes['quantidade'] = acoes['qtdisponivel'] + acoes['qtgarantia']
            del acoes['qtdisponivel']
            del acoes['qtgarantia']
            del acoes['valorfindisp']
            del acoes['valorfinemgar']
            acoes['produto'] = 'ações'
            acoes = acoes.rename(columns={'classeoperacao': 'a_p_op', 'puposicao': 'pu'})
            acoes['mtm_info'] = np.where(acoes['a_p_op'] == 'V', -1 * acoes['mtm_info'], acoes['mtm_info'])
            acoes['ativo'] = acoes['codativo']
            acoes = acoes[~acoes.mtm_info.isnull()]
            acoes = acoes[acoes['a_p_op'] != 'T']
            if len(acoes) > 0:
                quadro_oper = quadro_oper.append(acoes)

        except:
            return None

            # acoes: tributos
        try:

            query = 'select sum(tributos) as tributos from projeto_inv.xml_acoes a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_acoes where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            acoes_tributos = pd.read_sql(query, con=connection)
            acoes_tributos['mtm_info'] = -1 * acoes_tributos['tributos']
            del acoes_tributos['tributos']
            acoes_tributos['produto'] = 'tributos'
            acoes_tributos = acoes_tributos[~acoes_tributos.mtm_info.isnull()]
            if len(acoes_tributos) > 0:
                quadro_oper = quadro_oper.append(acoes_tributos)

        except:
            return None

        try:
            # caixa

            query = 'select saldo from projeto_inv.xml_caixa a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_caixa where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            caixa = pd.read_sql(query, con=connection)

            caixa['produto'] = 'caixa'
            caixa = caixa.rename(columns={'saldo': 'mtm_info'})
            caixa = caixa[~caixa.mtm_info.isnull()]
            if len(caixa) > 0:
                quadro_oper = quadro_oper.append(caixa)

        except:
            return None

        try:
            # ajuste de futuro

            query = 'select vlajuste, classeoperacao, quantidade, dtvencimento ,isin from projeto_inv.xml_futuros a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_futuros where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            futuro = pd.read_sql(query, con=connection)

            futuro = futuro.rename(
                columns={'vlajuste': 'mtm_info', 'classeoperacao': 'a_p_op', 'dtvencimento': 'dt_vencto'})
            futuro['produto'] = 'ajuste de futuro'
            futuro['pu'] = np.where(futuro['quantidade'] != 0, futuro['mtm_info'] / futuro['quantidade'], 0)
            futuro = futuro[~futuro.mtm_info.isnull()]
            if len(futuro) > 0:
                quadro_oper = quadro_oper.append(futuro)


        except:
            return None

        try:
            # futuro - tributos

            query = 'select sum(tributos) as tributos from projeto_inv.xml_futuros a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_futuros where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            futuro_tributos = pd.read_sql(query, con=connection)
            futuro_tributos['mtm_info'] = -1 * futuro_tributos['tributos']
            del futuro_tributos['tributos']
            futuro_tributos['produto'] = 'tributos'
            futuro_tributos = futuro_tributos[~futuro_tributos.mtm_info.isnull()]
            if len(futuro_tributos) > 0:
                quadro_oper = quadro_oper.append(futuro_tributos)


        except:
            return None

        try:
            # exposicao de futuro
            query = 'select vltotalpos, ativo, serie, classeoperacao, quantidade, dtvencimento ,isin, pu_regra_xml from projeto_inv.xml_futuros a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_futuros where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            futuro_expo = pd.read_sql(query, con=connection)

            futuro_expo = futuro_expo.rename(
                columns={'vltotalpos': 'mtm_info', 'classeoperacao': 'a_p_op', 'dtvencimento': 'dt_vencto'})
            futuro_expo['produto'] = 'Futuro'
            futuro_expo['mtm_info'] = np.where(futuro_expo['a_p_op'] == 'V', -1 * futuro_expo['mtm_info'],
                                               futuro_expo['mtm_info'])
            futuro_expo['ativo'] = futuro_expo['ativo'] + "_" + futuro_expo['serie']
            futuro_expo['pu'] = np.where(futuro_expo['quantidade'] != 0,
                                         futuro_expo['mtm_info'] / futuro_expo['quantidade'], 0)
            del futuro_expo['serie']
            futuro_expo = futuro_expo[~futuro_expo.mtm_info.isnull()]
            if len(futuro_expo) > 0:
                quadro_oper = quadro_oper.append(futuro_expo)


        except:
            return None

        try:
            # termo_rf

            query = 'select valorfin, dtoper, puposicao,	classeoperacao, qtd, dtvencimento, indexador, percindex, coupom , isin, principal, pu_mercado, pu_curva, pu_regra_xml from projeto_inv.xml_termorf a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_termorf where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            termorf = pd.read_sql(query, con=connection)

            termorf['mtm_info'] = termorf['valorfin'].astype(float)
            termorf = termorf.rename(
                columns={'classeoperacao': 'a_p_op', 'dtoper': 'dt_compra', 'dtvencimento': 'dt_vencto',
                         'qtd': 'quantidade', 'percindex': 'perc_index', 'coupom': 'tx_operacao', 'puposicao': 'pu'})
            termorf['produto'] = 'Termo_RF_' + termorf['indexador']
            termorf['mtm_info'] = np.where(termorf['a_p_op'] == 'V', -1 * termorf['mtm_info'], termorf['mtm_info'])
            del termorf['valorfin']
            termorf = termorf[~termorf.mtm_info.isnull()]
            if len(termorf) > 0:
                quadro_oper = quadro_oper.append(termorf)

        except:
            return None

        try:
            # termo_rf - tributos

            query = 'select sum(tributos) as tributos from projeto_inv.xml_termorf a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_termorf where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            termorf_tributos = pd.read_sql(query, con=connection)
            termorf_tributos['mtm_info'] = -1 * termorf_tributos['tributos']
            del termorf_tributos['tributos']
            termorf_tributos['produto'] = 'tributos'
            termorf_tributos = termorf_tributos[~termorf_tributos.mtm_info.isnull()]
            if len(termorf_tributos) > 0:
                quadro_oper = quadro_oper.append(termorf_tributos)

        except:
            return None

        try:

            # termo_rv

            query = 'select valorfinanceiro, ativo, puposicao,	dtoperacao, classeoperacao, quantidade, dtvencimento, isin, pu_regra_xml from projeto_inv.xml_termorv a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_termorv where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            termorv = pd.read_sql(query, con=connection)

            termorv['mtm_info'] = termorv['valorfinanceiro'].astype(float)
            termorv = termorv.rename(
                columns={'classeoperacao': 'a_p_op', 'dtoperacao': 'dt_compra', 'dtvencimento': 'dt_vencto',
                         'qtd': 'quantidade', 'puposicao': 'pu'})
            termorv['produto'] = 'Termo_RV_' + termorv['ativo']
            del termorv['valorfinanceiro']
            del termorv['ativo']
            #       termorv['mtm_info']=np.where(termorv['a_p_op']=='V',-1*termorv['mtm_info'],termorv['mtm_info'] )
            termorv = termorv[~termorv.mtm_info.isnull()]
            if len(termorv) > 0:
                quadro_oper = quadro_oper.append(termorv)


        except:
            return None

        try:
            # termo_rv - tributos

            query = 'select sum(tributos) as tributos from projeto_inv.xml_termorv a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_termorv where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            termorv_tributos = pd.read_sql(query, con=connection)
            termorv_tributos['mtm_info'] = -1 * termorv_tributos['tributos']
            del termorv_tributos['tributos']
            termorv_tributos['produto'] = 'tributos'
            termorv_tributos = termorv_tributos[~termorv_tributos.mtm_info.isnull()]
            if len(termorv_tributos) > 0:
                quadro_oper = quadro_oper.append(termorv_tributos)

        except:
            return None

        try:
            # swap - ativo

            query = 'select vlmercadoativo, dtoperacao, dtvencimento,	indexadorativo, percindexativo, taxaativo, cnpjcontraparte, isin , mtm_ativo_regra_xml from projeto_inv.xml_swap a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_swap where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            swap_ativo = pd.read_sql(query, con=connection)

            swap_ativo = swap_ativo.rename(
                columns={'vlmercadoativo': 'mtm_info', 'dtoperacao': 'dt_compra', 'dtvencimento': 'dt_vencto',
                         'indexadorativo': 'indexador', 'percindexativo': 'perc_index', 'cnpjcontraparte': 'cnpj',
                         'taxaativo': 'tx_operacao', 'mtm_ativo_regra_xml': 'mtm_regra_xml'})
            swap_ativo['produto'] = 'swap'
            swap_ativo = swap_ativo[~swap_ativo.mtm_info.isnull()]

            if len(swap_ativo) > 0:
                quadro_oper = quadro_oper.append(swap_ativo)


        except:
            return None

        try:
            # swap - passivo

            query = 'select vlmercadopassivo, dtoperacao, dtvencimento, indexadorpassivo, percindexpassivo, taxapassivo, cnpjcontraparte, isin , mtm_passivo_regra_xml from projeto_inv.xml_swap a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_swap where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            swap_passivo = pd.read_sql(query, con=connection)

            swap_passivo['mtm_info'] = -1 * swap_passivo['vlmercadopassivo'].astype(float)
            swap_passivo = swap_passivo.rename(
                columns={'dtoperacao': 'dt_compra', 'dtvencimento': 'dt_vencto', 'indexadorpassivo': 'indexador',
                         'cnpjcontraparte': 'cnpj', 'percindexpassivo': 'perc_index', 'taxapassivo': 'tx_operacao',
                         'mtm_passivo_regra_xml': 'mtm_regra_xml'})
            swap_passivo['produto'] = 'swap'
            del swap_passivo['vlmercadopassivo']
            swap_passivo = swap_passivo[~swap_passivo.mtm_info.isnull()]

            if len(swap_passivo) > 0:
                quadro_oper = quadro_oper.append(swap_passivo)


        except:
            return None

        try:
            # swap - tributos

            query = 'select sum(tributos) as tributos from projeto_inv.xml_swap a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_swap where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            swap_tributos = pd.read_sql(query, con=connection)
            swap_tributos['mtm_info'] = -1 * swap_tributos['tributos']
            del swap_tributos['tributos']
            swap_tributos['produto'] = 'tributos'
            swap_tributos = swap_tributos[~swap_tributos.mtm_info.isnull()]
            if len(swap_tributos) > 0:
                quadro_oper = quadro_oper.append(swap_tributos)

        except:
            return None

        try:
            # despesas

            query = 'select txadm from projeto_inv.xml_despesas a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_despesas where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            desp_adm = pd.read_sql(query, con=connection)

            desp_adm['mtm_info'] = -1 * desp_adm['txadm']
            desp_adm['produto'] = 'despesas-taxa de adm.'
            del desp_adm['txadm']
            desp_adm = desp_adm[~desp_adm.mtm_info.isnull()]
            desp_adm['flag_desp'] = 1
            if len(desp_adm) > 0:
                quadro_oper = quadro_oper.append(desp_adm)

        except:
            return None

        try:

            query = 'select tributos from projeto_inv.xml_despesas a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_despesas where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            desp_tributos = pd.read_sql(query, con=connection)

            desp_tributos['mtm_info'] = -1 * desp_tributos['tributos']
            desp_tributos['produto'] = 'despesas-tributos'
            del desp_tributos['tributos']
            desp_tributos = desp_tributos[~desp_tributos.mtm_info.isnull()]
            desp_tributos['flag_desp'] = 1
            desp_tributos = desp_tributos[~desp_tributos.mtm_info.isnull()]
            if len(desp_tributos) > 0:
                quadro_oper = quadro_oper.append(desp_tributos)

        except:
            return None

        try:

            query = 'select vltxperf from projeto_inv.xml_despesas a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_despesas where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            desp_perf = pd.read_sql(query, con=connection)

            desp_perf['mtm_info'] = -1 * desp_perf['vltxperf']
            desp_perf['produto'] = 'despesas-taxa de perfom.'
            del desp_perf['vltxperf']
            desp_perf = desp_perf[~desp_perf.mtm_info.isnull()]
            desp_perf['flag_desp'] = 1
            if len(desp_perf) > 0:
                quadro_oper = quadro_oper.append(desp_perf)

        except:
            return None

        try:

            query = 'select outtax from projeto_inv.xml_despesas a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_despesas where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            desp_outras = pd.read_sql(query, con=connection)

            desp_outras['mtm_info'] = -1 * desp_outras['outtax']
            desp_outras['produto'] = 'despesas-outras taxas'
            del desp_outras['outtax']
            desp_outras = desp_outras[~desp_outras.mtm_info.isnull()]
            desp_outras['flag_desp'] = 1
            if len(desp_outras) > 0:
                quadro_oper = quadro_oper.append(desp_outras)

        except:
            return None

            # outras despesas

        try:
            query = 'select valor, coddesp from projeto_inv.xml_outrasdespesas a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_outrasdespesas where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            outras_desp = pd.read_sql(query, con=connection)

            query = 'select a.coddesp, a.descricao, a.tipo_registro from projeto_inv.anbima_coddesp a right join (select max(data_bd) as data_bd from projeto_inv.anbima_coddesp) b on a.data_bd=b.data_bd;'
            coddesp = pd.read_sql(query, con=connection)

            outras_desp = pd.merge(outras_desp, coddesp, left_on='coddesp', right_on='coddesp', how='left')
            outras_desp = outras_desp[outras_desp.coddesp.notnull()]
            if len(outras_desp) > 0:
                outras_desp['mtm_info'] = np.where(outras_desp['tipo_registro'] == 'C', outras_desp['valor'],
                                                   -1 * outras_desp['valor'])
                outras_desp['produto'] = 'outras despesas ' + outras_desp['descricao']
                del outras_desp['valor']
                del outras_desp['descricao']
                del outras_desp['coddesp']
                del outras_desp['tipo_registro']
                outras_desp = outras_desp[~outras_desp.mtm_info.isnull()]
                outras_desp['flag_desp'] = 1
                if len(outras_desp) > 0:
                    quadro_oper = quadro_oper.append(outras_desp)
        except:
            return None

        try:
            # corretagem

            query = 'select vlbov, vlrepassebov, vlbmf, vlrepassebmf, vloutbolsas, vlrepasseoutbol from projeto_inv.xml_corretagem a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_corretagem where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            corretagem = pd.read_sql(query, con=connection)

            corretagem['mtm_info'] = -1 * (
            corretagem['vlbov'] + corretagem['vlrepassebov'] + corretagem['vlbmf'] + corretagem['vlrepassebmf'] +
            corretagem['vloutbolsas'] + corretagem['vlrepasseoutbol'])
            corretagem['produto'] = 'Corretagem'
            corretagem_f = corretagem[['mtm_info', 'produto']]
            corretagem_f = corretagem_f[~corretagem_f.mtm_info.isnull()]
            corretagem_f['flag_desp'] = 1
            if len(corretagem_f) > 0:
                quadro_oper = quadro_oper.append(corretagem_f)
        except:
            return None

        try:
            # opcoes derivativos

            query = 'select valorfinanceiro, ativo, serie, callput, classeoperacao,puposicao, quantidade, dtvencimento, isin, pu_regra_xml from projeto_inv.xml_opcoesderiv a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_opcoesderiv where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            opcoesderiv = pd.read_sql(query, con=connection)

            opcoesderiv = opcoesderiv.rename(
                columns={'valorfinanceiro': 'mtm_info', 'classeoperacao': 'a_p_op', 'dtvencimento': 'dt_vencto',
                         'puposicao': 'pu'})
            opcoesderiv['produto'] = 'Opções_' + opcoesderiv['ativo'] + " " + opcoesderiv['callput']
            opcoesderiv['ativo'] = opcoesderiv['ativo'] + "_" + opcoesderiv['serie']
            del opcoesderiv['callput']
            del opcoesderiv['serie']
            opcoesderiv['mtm_info'] = np.where(opcoesderiv['a_p_op'] == 'V', -1 * opcoesderiv['mtm_info'],
                                               opcoesderiv['mtm_info'])
            opcoesderiv = opcoesderiv[~opcoesderiv.mtm_info.isnull()]

            if len(opcoesderiv) > 0:
                quadro_oper = quadro_oper.append(opcoesderiv)
        except:
            return None

        try:
            # opcoes derivativos- tributos

            query = 'select sum(tributos) as tributos from projeto_inv.xml_opcoesderiv a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_opcoesderiv where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            opcoesderiv_trib = pd.read_sql(query, con=connection)
            opcoesderiv_trib['mtm_info'] = -1 * opcoesderiv_trib['tributos']
            del opcoesderiv_trib['tributos']
            opcoesderiv_trib['produto'] = 'tributos'
            opcoesderiv_trib = opcoesderiv_trib[~opcoesderiv_trib.mtm_info.isnull()]
            if len(opcoesderiv_trib) > 0:
                quadro_oper = quadro_oper.append(opcoesderiv_trib)

        except:
            return None

        try:

            # opcoes acoes

            query = 'select valorfinanceiro, codativo, classeoperacao, puposicao, qtdisponivel, dtvencimento, isin, pu_regra_xml from projeto_inv.xml_opcoesacoes a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_opcoesacoes where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            opcoesacoes = pd.read_sql(query, con=connection)

            opcoesacoes['produto'] = 'Opções de ações-' + opcoesacoes['codativo']
            opcoesacoes = opcoesacoes.rename(
                columns={'valorfinanceiro': 'mtm_info', 'classeoperacao': 'a_p_op', 'dtvencimento': 'dt_vencto',
                         'qtdisponivel': 'quantidade', 'codativo': 'ativo', 'puposicao': 'pu'})
            opcoesacoes['mtm_info'] = np.where(opcoesacoes['a_p_op'] == 'V', -1 * opcoesacoes['mtm_info'],
                                               opcoesacoes['mtm_info'])
            opcoesacoes = opcoesacoes[~opcoesacoes.mtm_info.isnull()]

            if len(opcoesacoes) > 0:
                quadro_oper = quadro_oper.append(opcoesacoes)
        except:
            return None

        try:

            # opcoes acoes- tributos

            query = 'select sum(tributos) as tributos from projeto_inv.xml_opcoesacoes a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_opcoesacoes where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            opcoesacoes_trib = pd.read_sql(query, con=connection)
            opcoesacoes_trib['mtm_info'] = -1 * opcoesacoes_trib['tributos']
            del opcoesacoes_trib['tributos']
            opcoesacoes_trib['produto'] = 'tributos'
            opcoesacoes_trib = opcoesacoes_trib[~opcoesacoes_trib.mtm_info.isnull()]
            if len(opcoesacoes_trib) > 0:
                quadro_oper = quadro_oper.append(opcoesacoes_trib)

        except:
            return None

        try:

            # opcoes flexiveis

            query = 'select valorfinanceiro, callput, ativo, tipo, dtoperacao, classeoperacao, puposicao, quantidade, garantia, isin, pu_regra_xml from projeto_inv.xml_opcoesflx a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_opcoesflx where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            opcoesflx = pd.read_sql(query, con=connection)

            opcoesflx['produto'] = 'Opções de ações-' + opcoesflx['callput'] + "-" + opcoesflx['ativo'] + "-" + opcoesflx[
                'tipo']
            opcoesflx = opcoesflx.rename(
                columns={'valorfinanceiro': 'mtm_info', 'classeoperacao': 'a_p_op', 'puposicao': 'pu'})
            del opcoesflx['callput']
            del opcoesflx['tipo']
            opcoesflx['mtm_info'] = np.where(opcoesflx['a_p_op'] == 'V', -1 * opcoesflx['mtm_info'], opcoesflx['mtm_info'])
            opcoesflx = opcoesflx[~opcoesflx.mtm_info.isnull()]

            if len(opcoesflx) > 0:
                quadro_oper = quadro_oper.append(opcoesflx)
        except:
            return None

        try:

            # opcoes flexiveis- tributos

            query = 'select sum(tributos) as tributos from projeto_inv.xml_opcoesflx a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_opcoesflx where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            opcoesflx_trib = pd.read_sql(query, con=connection)
            opcoesflx_trib['mtm_info'] = -1 * opcoesflx_trib['tributos']
            del opcoesflx_trib['tributos']
            opcoesflx_trib['produto'] = 'tributos'
            opcoesflx_trib = opcoesflx_trib[~opcoesflx_trib.mtm_info.isnull()]
            if len(opcoesflx_trib) > 0:
                quadro_oper = quadro_oper.append(opcoesflx_trib)

        except:
            return None

        try:
            # header


            query = 'select cnpj, valorreceber, valorpagar, vlcotasemitir, vlcotasresgatar  from projeto_inv.xml_header a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_header where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            header = pd.read_sql(query, con=connection)

            valorreceber = header[['valorreceber', 'cnpj']]
            valorreceber['produto'] = 'valores a receber'
            valorreceber = valorreceber.rename(columns={'valorreceber': 'mtm_info', 'cnpj': 'cnpjfundo'})
            valorreceber['flag_desp'] = 0

            valorpagar = header[['valorpagar', 'cnpj']]
            valorpagar['produto'] = 'valores a pagar'
            valorpagar['valorpagar'] = -1 * valorpagar['valorpagar']
            valorpagar = valorpagar.rename(columns={'valorpagar': 'mtm_info', 'cnpj': 'cnpjfundo'})
            valorpagar['flag_desp'] = 0

            vlcotasemitir = header[['vlcotasemitir', 'cnpj']]
            vlcotasemitir['produto'] = 'valor das cotas a emitir'
            vlcotasemitir['vlcotasemitir'] = -1 * vlcotasemitir['vlcotasemitir']
            vlcotasemitir = vlcotasemitir.rename(columns={'vlcotasemitir': 'mtm_info', 'cnpj': 'cnpjfundo'})
            vlcotasemitir['flag_desp'] = 0

            vlcotasresgatar = header[['vlcotasresgatar', 'cnpj']]
            vlcotasresgatar['produto'] = 'valor das cotas a resgatar'
            vlcotasresgatar['vlcotasresgatar'] = -1 * vlcotasresgatar['vlcotasresgatar']
            vlcotasresgatar = vlcotasresgatar.rename(columns={'vlcotasresgatar': 'mtm_info', 'cnpj': 'cnpjfundo'})
            vlcotasresgatar['flag_desp'] = 0

            if len(valorreceber) > 0:
                quadro_oper = quadro_oper.append(valorreceber)
            if len(valorpagar) > 0:
                quadro_oper = quadro_oper.append(valorpagar)
            if len(vlcotasemitir) > 0:
                quadro_oper = quadro_oper.append(vlcotasemitir)
            if len(vlcotasresgatar) > 0:
                quadro_oper = quadro_oper.append(vlcotasresgatar)

            # nome da empresa
            emissor_final = emissor2
            quadro_oper = quadro_oper.merge(emissor_final, on=['cnpj'], how='left')
        except:
            return None

        try:
            # cotas

            query = 'select a.* from projeto_inv.xml_cotas a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_cotas where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            cotas = pd.read_sql(query, con=connection)

            cotas['quantidade'] = cotas['qtdisponivel'] + cotas['qtgarantia']
            cotas['mtm_info_fundo'] = cotas['quantidade'] * cotas['puposicao']
            # cotas['mtm_info']=cotas['qtdisponivel']*cotas['puposicao']
            cotas = cotas.rename(columns={'puposicao': 'pu'})
            cotas_fim = cotas[['mtm_info_fundo', 'isin', 'cnpjfundo', 'quantidade', 'pu', 'pu_regra_xml']]
            cotas_fim['produto'] = 'fundo'
            cotas_fim = cotas_fim[~cotas_fim.mtm_info_fundo.isnull()]

            if len(cotas_fim) > 0:
                quadro_oper = quadro_oper.append(cotas_fim)
        except:
            return None

        try:

            # cotas tributos
            query = 'select a.* from (select header_id, data_bd, cnpjfundo, sum(tributos) as tributos from projeto_inv.xml_cotas group by 1,2,3) a right join (select header_id, max(data_bd) as data_bd1 from projeto_inv.xml_cotas where header_id=' + header_id + ') b on a.data_bd=b.data_bd1 and a.header_id=b.header_id;'
            cotas_tributos = pd.read_sql(query, con=connection)
            cotas_tributos = cotas_tributos[~cotas_tributos.tributos.isnull()]
            cotas_tributos['flag_desp'] = 1
            cotas_tributos['mtm_info_fundo'] = -1 * cotas_tributos['tributos']
            cotas_tributos['produto'] = 'tributos_cotas_fundo'
            cotas_tributos_fim = cotas_tributos[['mtm_info_fundo', 'produto', 'cnpjfundo', 'flag_desp']]
            cotas_tributos_fim = cotas_tributos_fim[~cotas_tributos_fim.mtm_info_fundo.isnull()]

            if len(cotas_tributos_fim) > 0:
                quadro_oper = quadro_oper.append(cotas_tributos_fim)

            quadro_oper['header_id'] = header_id
            quadro_oper['patliq'] = patliq
            quadro_oper = quadro_oper.reindex(
                columns=['mtm_info', 'produto', 'dt_emissao', 'dt_compra', 'cnpj', 'contraparte', 'a_p_op', 'quantidade',
                         'dt_vencto', 'indexador', 'perc_index', 'tx_operacao', 'cnpjfundo', 'fundo', 'mtm_info_fundo',
                         'isin', 'ativo', 'caracteristica', 'header_id', 'patliq', 'pu', 'pu_mercado', 'pu_curva',
                         'pu_regra_xml', 'mtm_regra_xml', 'flag_desp'])
            quadro_oper['fundo'] = nome_fundo
        except:
            return None

    # OBSERVAÇÃO: mtm_xml_regra : somente para swap. Para os demais, multiplicar por pu. Compromissada utilizar informação disponibilizada no xml

    def conso_xml(dt_base, cnpjcpf):
        tratamento_xml(dt_base, cnpjcpf)
        global quadro_operacoes
        quadro_operacoes = quadro_oper.copy()

    # rodar código HDI seguros - carteira consolidada
    def parametrizacao(dt_base, cnpj):
        header_xml(dt_base, cnpj)
        # dt_base= "2016-02-29"
        # conso_xml(dt_base,'29980158000157')
        global lista_fundos
        global quadro_operacoes
        global fundos
        global demais_ativos
        global fundos1

        conso_xml(dt_base, cnpj)
        # selecao de todos os xmls recebidos
        senhabd = "projetoinvbd"
        query = 'select distinct cnpj from projeto_inv.xml_header where cnpj<> "" and dtposicao=' + '"' + dt_base + '";'

        lista_fundos = pd.read_sql(query, con=connection)

        quadro_operacoes = quadro_oper.copy()
        quadro_operacoes['cnpjfundo_xml'] = quadro_operacoes['cnpjfundo']

        fundos = quadro_operacoes[quadro_operacoes.produto == 'fundo']
        demais_ativos = quadro_operacoes[quadro_operacoes.produto != 'fundo']
        fundos1 = fundos.rename(columns={'mtm_info_fundo': 'mtm_info_fundo_posicao_cl', 'quantidade': 'quantidade_fundos',
                                         'isin': 'isin_fundos', 'pu': 'pu_xml', 'pu_regra_xml': 'pu_regra_xml_posicao_cl'})
        fundos1 = fundos1[['mtm_info_fundo_posicao_cl', 'cnpjfundo_xml', 'quantidade_fundos', 'isin_fundos', 'pu_xml',
                           'pu_regra_xml_posicao_cl']]

        # def fundos(dt_base1):
        global quadro_conso_fundos
        quadro_conso_fundos = pd.DataFrame(
            columns=['mtm_info', 'produto', 'dt_emissao', 'dt_compra', 'cnpj', 'contraparte', 'a_p_op', 'quantidade',
                     'dt_vencto', 'indexador', 'perc_index', 'tx_operacao', 'cnpjfundo', 'fundo', 'mtm_info_fundo', 'isin',
                     'caracteristica', 'header_id', 'patliq', 'cnpjfundo_xml', 'pu_curva', 'pu_regra_xml'])
        y = 0

        #    senhabd="projetoinvbd"
        #    connection=db.connect('localhost', user = 'root', passwd = senhabd, db = 'projeto_inv', charset='utf8')
        #    x='select distinct cnpj from projeto_inv.xml_header where cnpj>0 and dtposicao='+'"'+dt_base1+'";'

        #    lista_fundos=pd.read_sql(x, con=connection)

        for y in range(len(lista_fundos)):
            # x=lista_fundos.get_value(y,'cnpj').astype(str)
            x = lista_fundos.get_value(y, 'cnpj')
            tratamento_xml(dt_base, "'" + x + "'")

            globals()['fundos_%s' % x] = quadro_oper.copy()
            # globals() ['fundos_%s' %x]['cnpjfundo_xml']=x.astype(float)
            globals()['fundos_%s' % x]['cnpjfundo_xml'] = x
            globals()['fundos_%s' % x]['linhas'] = len(globals()['fundos_%s' % x])
            quadro_conso_fundos = quadro_conso_fundos.append(globals()['fundos_%s' % x])

        global fundos_cliente
        global fundos_cliente_final
        fundos_cliente = fundos1.copy()
        fundos_cliente_final = pd.merge(fundos_cliente, quadro_conso_fundos, how='left', left_on='cnpjfundo_xml',
                                        right_on='cnpjfundo_xml')
        fundos_cliente_final = fundos_cliente_final.rename(
            columns={'mtm_info': 'mtm_info_xml', 'mtm_info_fundo': 'mtm_info_fundo_xml', 'quantidade': 'quantidade_xml'})
        # fundos_cliente_final['mtm_info']=np.where(np.isnan(fundos_cliente_final.mtm_info_xml) & fundos_cliente_final.cnpjfundo_xml.notnull() & fundos_cliente_final.produto.notnull(), fundos_cliente_final['mtm_info_fundo_posicao_cl'], fundos_cliente_final['mtm_info_fundo_posicao_cl']/fundos_cliente_final['patliq']*fundos_cliente_final['mtm_info_xml'])
        # fundos_cliente_final['mtm_info_fundo']=np.where(np.isnan(fundos_cliente_final.mtm_info_xml) & fundos_cliente_final.cnpjfundo_xml.notnull() & fundos_cliente_final.produto.notnull(),fundos_cliente_final['mtm_info_fundo_posicao_cl'], fundos_cliente_final['mtm_info_fundo_posicao_cl']/fundos_cliente_final['patliq']*fundos_cliente_final['mtm_info_fundo_xml'])

        fundos_cliente_final['mtm_info'] = fundos_cliente_final['mtm_info_fundo_posicao_cl'] / fundos_cliente_final[
            'patliq'] * fundos_cliente_final['mtm_info_xml']
        fundos_cliente_final['mtm_info_fundo'] = fundos_cliente_final['mtm_info_fundo_posicao_cl'] / fundos_cliente_final[
            'patliq'] * fundos_cliente_final['mtm_info_fundo_xml']
        fundos_cliente_final['produto'] = fundos_cliente_final['produto'].fillna('fundo')
        fundos_cliente_final['quantidade'] = np.where(fundos_cliente_final['patliq'] != 0,
                                                      fundos_cliente_final['mtm_info_fundo_posicao_cl'] /
                                                      fundos_cliente_final['patliq'] * fundos_cliente_final[
                                                          'quantidade_xml'], 0)

        # fundos_cliente_final['quantidade']=np.where(np.isnan(fundos_cliente_final.quantidade), fundos_cliente_final.quantidade_xml, fundos_cliente_final.quantidade)
        # fundos_cliente_final['mtm_info']=np.where(fundos_cliente_final['produto']=='fundo',fundos_cliente_final['mtm_info_fundo'], fundos_cliente_final['mtm_info'])
        global quadro_conso

        quadro_conso = demais_ativos.copy()
        quadro_conso = quadro_conso.append(fundos_cliente_final)
        quadro_conso = quadro_conso.reindex(
            columns=['mtm_info', 'produto', 'dt_emissao', 'dt_compra', 'cnpj', 'contraparte', 'a_p_op', 'quantidade',
                     'dt_vencto', 'indexador', 'perc_index', 'tx_operacao', 'cnpjfundo', 'fundo', 'mtm_info_fundo', 'isin',
                     'ativo', 'caracteristica', 'header_id', 'patliq', 'cnpjfundo_xml', 'mtm_info_fundo_posicao_cl',
                     'mtm_info_fundo_xml', 'mtm_info_xml', 'linhas', 'quantidade_fundos', 'isin_fundos', 'pu', 'pu_xml',
                     'pu_mercado', 'pu_curva', 'pu_regra_xml', 'flag_desp'])
        quadro_conso['linhas'] = quadro_conso['linhas'].fillna(1)
        quadro_conso['mtm_info'] = np.where((quadro_conso['linhas'] == 1) & (quadro_conso['produto'] == 'fundo'),
                                            quadro_conso['mtm_info_fundo_posicao_cl'], quadro_conso['mtm_info'])
        quadro_conso['mtm_info'] = np.where((quadro_conso['linhas'] > 1) & (quadro_conso['produto'] == 'fundo'),
                                            quadro_conso['mtm_info_fundo'], quadro_conso['mtm_info'])
        quadro_conso['quantidade'] = np.where((quadro_conso['linhas'] == 1) & (quadro_conso['produto'] == 'fundo'),
                                              quadro_conso['quantidade_fundos'], quadro_conso['quantidade'])

        quadro_conso['pu'] = quadro_conso['pu'].fillna(quadro_conso['pu_xml'])

        # demais níveis
        qtde_linhas = len(quadro_conso)
        qtde_linhas2 = qtde_linhas + 1

        global fundos_niveis
        global demais_niveis

        quadro_conso['cnpjfundo_1nivel'] = quadro_conso['cnpjfundo_xml']
        while qtde_linhas2 > qtde_linhas:
            qtde_linhas = len(quadro_conso)

            fundos_niveis = quadro_conso[(quadro_conso['produto'] == 'fundo') & (quadro_conso['linhas'] > 1)]
            demais_niveis = quadro_conso[(quadro_conso['produto'] != 'fundo') | (quadro_conso['linhas'] <= 1)]

            f_cliente = fundos_niveis[['mtm_info', 'cnpjfundo', 'cnpjfundo_1nivel', 'isin', 'quantidade', 'pu']]
            f_cliente1 = f_cliente.copy()
            f_cliente1 = f_cliente1.rename(
                columns={'mtm_info': 'mtm_info_fundo_posicao_cl', 'cnpjfundo': 'cnpjfundo_outros',
                         'quantidade': 'quantidade_fundos', 'isin': 'isin_fundos', 'pu': 'pu_xml'})
            f_cliente2 = pd.merge(f_cliente1, quadro_conso_fundos, how='left', left_on='cnpjfundo_outros',
                                  right_on='cnpjfundo_xml')

            f_cliente2 = f_cliente2.rename(columns={'mtm_info': 'mtm_info_xml', 'mtm_info_fundo': 'mtm_info_fundo_xml',
                                                    'quantidade': 'quantidade_xml'})
            f_cliente2['mtm_info'] = f_cliente2['mtm_info_fundo_posicao_cl'] / f_cliente2['patliq'] * f_cliente2[
                'mtm_info_xml']
            f_cliente2['mtm_info_fundo'] = f_cliente2['mtm_info_fundo_posicao_cl'] / f_cliente2['patliq'] * f_cliente2[
                'mtm_info_fundo_xml']

            f_cliente2['produto'] = f_cliente2['produto'].fillna('fundo')
            f_cliente2['linhas'] = f_cliente2['linhas'].fillna(1)
            f_cliente2['mtm_info'] = np.where((f_cliente2['linhas'] == 1) & (f_cliente2['produto'] == 'fundo'),
                                              f_cliente2['mtm_info_fundo_posicao_cl'], f_cliente2['mtm_info'])
            f_cliente2['mtm_info'] = np.where((f_cliente2['linhas'] > 1) & (f_cliente2['produto'] == 'fundo'),
                                              f_cliente2['mtm_info_fundo'], f_cliente2['mtm_info'])
            f_cliente2['quantidade'] = np.where(f_cliente2['patliq'] != 0,
                                                f_cliente2['mtm_info_fundo_posicao_cl'] / f_cliente2['patliq'] * f_cliente2[
                                                    'quantidade_xml'], 0)
            f_cliente2['quantidade'] = np.where((f_cliente2['linhas'] == 1) & (f_cliente2['produto'] == 'fundo'),
                                                f_cliente2['quantidade_xml'], f_cliente2['quantidade'])
            f_cliente2['pu'] = f_cliente2['pu'].fillna(f_cliente2['pu_xml'])
            quadro_conso = demais_niveis.append(f_cliente2)
            qtde_linhas2 = len(quadro_conso)

        del quadro_conso['cnpjfundo']
        del quadro_conso['fundo']
        del quadro_conso['mtm_info_fundo']

        del quadro_conso['patliq']
        del quadro_conso['cnpjfundo_xml']
        del quadro_conso['mtm_info_fundo_posicao_cl']
        del quadro_conso['mtm_info_fundo_xml']
        del quadro_conso['mtm_info_xml']
        del quadro_conso['linhas']

        # nome do fundo
        global nome_fundo
        nome_fundo = pd.read_csv(depend_path_SPW_FI, sep='\t', header=0, encoding="iso-8859-1")
        nome_fundo_fim = nome_fundo[['DENOMINACAO_SOCIAL', 'CNPJ']]
        nome_fundo_fim = nome_fundo_fim.rename(columns={'DENOMINACAO_SOCIAL': 'fundo'})
        nome_fundo_fim['cnpj_fundo'] = nome_fundo_fim['CNPJ'].astype(str)
        nome_fundo_fim['cnpj_fundo'] = nome_fundo_fim['cnpj_fundo'].str.zfill(14)
        nome_fundo_fim = nome_fundo_fim.sort(['cnpj_fundo'], ascending=True)
        nome_fundo_fim = nome_fundo_fim.drop_duplicates(subset=['cnpj_fundo'])
        del nome_fundo_fim['CNPJ']

        estruturados = pd.read_excel(depend_path_fundos_estruturados)
        estruturados = estruturados.rename(columns={'denominacao_social': 'fundo'})
        estruturados['cnpj_fundo'] = estruturados['cnpj'].astype(str)
        estruturados['cnpj_fundo'] = estruturados['cnpj_fundo'].str.zfill(14)
        del estruturados['cnpj']
        nome_fundo_fim = nome_fundo_fim.append(estruturados)
        nome_fundo_fim = nome_fundo_fim.drop_duplicates(subset=['cnpj_fundo'])

        quadro_conso = pd.merge(quadro_conso, nome_fundo_fim, how='left', left_on='cnpjfundo_1nivel', right_on='cnpj_fundo')
        del quadro_conso['cnpj_fundo']
        nome_fundo_fim2 = nome_fundo_fim.copy()
        nome_fundo_fim2 = nome_fundo_fim2.rename(columns={'fundo': 'fundo_ult_nivel', 'CNPJ': 'cnpj_fundo'})
        quadro_conso = pd.merge(quadro_conso, nome_fundo_fim2, how='left', left_on='cnpjfundo_outros',
                                right_on='cnpj_fundo')
        del quadro_conso['cnpj_fundo']

        # header

        quadro_conso['header_id'] = header_id_carteira
        quadro_conso['data_bd'] = datetime.datetime.now()
        quadro_conso['isin'] = np.where(quadro_conso.produto == 'fundo', quadro_conso['isin_fundos'], quadro_conso['isin'])
        # quadro_conso['quantidade']=np.where(quadro_conso.produto=='fundo',quadro_conso['quantidade_fundos'],quadro_conso['quantidade'] )
        quadro_conso['mtm_info'] = quadro_conso['mtm_info'].fillna(0)
        quadro_conso['quantidade_calculada'] = quadro_conso['mtm_info'] / quadro_conso['pu']
        quadro_conso = quadro_conso.reindex(
            columns=['mtm_info', 'produto', 'dt_emissao', 'dt_compra', 'cnpj', 'contraparte', 'a_p_op',
                     'quantidade_calculada', 'dt_vencto', 'indexador', 'perc_index', 'tx_operacao', 'isin', 'ativo',
                     'caracteristica', 'cnpjfundo_1nivel', 'fundo', 'cnpjfundo_outros', 'fundo_ult_nivel', 'pu',
                     'pu_mercado', 'pu_curva', 'pu_regra_xml', 'header_id', 'flag_desp', 'data_bd'])

        quadro_conso.ix[quadro_conso.produto == 'caixa', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'despesas-taxa de adm.', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'despesas-tributos', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'despesas-taxa de perfom.', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'despesas-outras taxas', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'outras despesas', 'quantidade_calculada'] = 0
        #    quadro_conso.ix[quadro_conso.produto=='Provisão', 'quantidade_calculada']=0
        quadro_conso.ix[quadro_conso.produto == 'valores a receber', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'valores a pagar', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'valor das cotas a emitir', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'valor das cotas a resgatar', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'tributos_cotas_fundo', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'swap', 'quantidade_calculada'] = 0
        quadro_conso.ix[quadro_conso.produto == 'Corretagem', 'quantidade_calculada'] = 0

        quadro_conso = quadro_conso.rename(columns={'quantidade_calculada': 'quantidade'})

        # quadro_conso1=quadro_conso.drop_duplicates()


        quadro_conso = quadro_conso.replace({np.nan: None}, regex=True)

        quadro_conso['mtm_regra_xml'] = np.where((quadro_conso['mtm_info'] >= 0) & (~quadro_conso['pu_regra_xml'].isnull()),
                                                 quadro_conso['pu_regra_xml'] * quadro_conso['quantidade'].abs(),
                                                 -1 * quadro_conso['pu_regra_xml'] * quadro_conso['quantidade'].abs())
        ''''''
        quadro_conso.to_excel(save_path_validacao_quadroconso)

        quadro_conso['mtm_mercado'] = np.where((quadro_conso['mtm_info'] >= 0) & (~quadro_conso['pu_mercado'].isnull()),
                                               quadro_conso['pu_mercado'] * quadro_conso['quantidade'].abs(),
                                               -1 * quadro_conso['pu_mercado'] * quadro_conso['quantidade'].abs())
        quadro_conso['mtm_curva'] = np.where((quadro_conso['mtm_info'] >= 0) & (~quadro_conso['pu_curva'].isnull()),
                                             quadro_conso['pu_curva'] * quadro_conso['quantidade'].abs(),
                                             -1 * quadro_conso['pu_curva'] * quadro_conso['quantidade'].abs())
        ''''''
        #    global cotas_cvm
        #    global cotas_cvm1
        #    global cotas_cvm2
        #
        #    x="select a.* from (select * from projeto_inv.cvm_cotas where dt_ref='"+dt_base+"') a right join (select distinct cnpj_fundo, dt_ref, max(data_bd) as data_bd1 from projeto_inv.cvm_cotas  group by 1,2) b on a.data_bd=b.data_bd1 and a.cnpj_fundo=b.cnpj_fundo and a.dt_ref=b.dt_ref;"
        #
        #    cotas_cvm=pd.read_sql(x,con=connection)
        #    cotas_cvm=cotas_cvm.sort(columns=['cnpj_fundo','dt_ref', 'data_bd'], ascending=[True, True, False])
        #    cotas_cvm1=cotas_cvm.drop_duplicates(subset=['cnpj_fundo','dt_ref'], take_last=False)
        #    cotas_cvm2=cotas_cvm1[['cnpj_fundo', 'quota']]
        #
        #    quadro_conso['cnpj_fundo_final']=np.where((quadro_conso['produto']=='fundo') & (quadro_conso['cnpjfundo_outros'].isnull()), quadro_conso['cnpjfundo_1nivel'], np.where(quadro_conso.produto=='fundo', quadro_conso['cnpjfundo_outros'],""))
        #
        #    temp=pd.merge(quadro_conso, cotas_cvm2, left_on=['cnpj_fundo_final'], right_on=['cnpj_fundo'], how='right')
        #
        #    quadro_conso=pd.merge(quadro_conso, cotas_cvm2, left_on=['cnpj_fundo_final'], right_on=['cnpj_fundo'], how='left')
        #    quadro_conso['mtm_regra_xml']=np.where(~quadro_conso['quota'].isnull(), quadro_conso['quantidade']*quadro_conso['quota'], quadro_conso['mtm_regra_xml'])
        quadro_conso['flag_null'] = np.where(quadro_conso['mtm_regra_xml'].isnull(), 1, 0)
        quadro_conso['mtm_regra_xml'] = np.where(quadro_conso['flag_null'] == 1, quadro_conso['mtm_info'],
                                                 quadro_conso['mtm_regra_xml'])

        # cálculo de dias úteis

        # GERACAO DE SERIE DE DIAS ÚTEIS E DIAS CORRIDOS

        temp = quadro_conso[~quadro_conso['dt_vencto'].isnull()]
        dt_max = max(temp['dt_vencto'])
        dt_max = dt_max.strftime('%d-%m-%Y')
        dt_inicio = pd.to_datetime(dt_base).strftime('%d-%m-%Y')

        per = FinDt.DatasFinanceiras(dt_inicio, dt_max, path_arquivo=feriados_sheet)
        du = pd.DataFrame(columns=['data_ref'])
        dc = pd.DataFrame(columns=['data_ref'])
        dc['data_ref'] = per.dias()
        dc['flag_dc'] = 1
        du['data_ref'] = per.dias(3)
        du['flag_du'] = 1
        serie_dias = pd.merge(dc, du, left_on=['data_ref'], right_on=['data_ref'], how='left')
        serie_dias['flag_du'] = serie_dias['flag_du'].fillna(0)
        serie_dias['indice_dc'] = np.cumsum(serie_dias['flag_dc'])
        serie_dias['indice_du'] = np.cumsum(serie_dias['flag_du'])
        del serie_dias['flag_du']
        del serie_dias['flag_dc']

        serie = serie_dias[['data_ref', 'indice_du']]

        quadro_conso = pd.merge(quadro_conso, serie, left_on='dt_vencto', right_on='data_ref', how='left')

        quadro_conso = quadro_conso.rename(columns={'indice_du': 'du'})

        quadro_conso['flag_desp'] = quadro_conso['flag_desp'].fillna(0)

        x = 'select max(id_relatorio_qo) as id_relatorio_qo from projeto_inv.xml_quadro_operacoes;'
        id_qo = pd.read_sql(x, con=connection)

        if id_qo['id_relatorio_qo'][0] == None:
            id_relatorio_qo = 0
        else:
            id_relatorio_qo = id_qo.get_value(0, 'id_relatorio_qo').astype(int) + 1

        quadro_conso['id_relatorio_qo'] = id_relatorio_qo

        # del quadro_conso['cnpj_fundo_final']
        # del quadro_conso['cnpj_fundo']
        del quadro_conso['data_ref']

        quadro_conso_fim = quadro_conso[(quadro_conso['flag_desp'] == 0) & (quadro_conso['mtm_info'] != 0)].copy()
        quadro_despesas = quadro_conso[(quadro_conso['flag_desp'] == 1) & (quadro_conso['mtm_info'] != 0)].copy()
        global quadro_conso_fim
        global quadro_despesas

        # Ajuste de sinal na munheca
        quadro_conso_fim['a_p_op'] = np.where(
            (quadro_conso_fim['produto'].isin(['swap']) & (quadro_conso_fim['mtm_info'] < 0)), 'V',
            np.where((quadro_conso_fim['produto'].isin(['swap'])), 'C', quadro_conso_fim['a_p_op']))

        quadro_conso_fim['quantidade'][quadro_conso_fim.quantidade == np.inf] = 0
        quadro_conso_fim.replace(np.inf, np.nan, inplace=True)
        quadro_conso_fim.to_excel(end_val + 'quadro_conso_' + cnpj + '_' + header_id_carteira + "_" + dt_base + '.xlsx')
        quadro_despesas.to_excel(end_val + 'quadro_desp_' + cnpj + '_' + header_id_carteira + "_" + dt_base + '.xlsx')
        del quadro_conso_fim['flag_desp']
        del quadro_despesas['flag_desp']

        quadro_conso_fim.replace((-np.inf), np.nan, inplace=True)
        quadro_conso_fim.replace(np.inf, np.nan, inplace=True)
        pd.io.sql.to_sql(quadro_conso_fim, name='xml_quadro_operacoes', con=connection, if_exists="append", flavor='mysql',
                         index=0)
        pd.io.sql.to_sql(quadro_despesas, name='xml_quadro_despesas', con=connection, if_exists="append", flavor='mysql',
                         index=0)

    def fundosprimeironivel(dt, cnpj):
        query = 'select * from projeto_inv.xml_header where cnpjcpf="' + cnpj + '" and dtposicao=' + '"' + dt_base + '";'

        df = pd.read_sql(query, con=connection)
        if len(df) == 0:
            query = 'select * from projeto_inv.xml_header where cnpj="' + cnpj + '" and dtposicao=' + '"' + dt_base + '";'
            df = pd.read_sql(query, con=connection)
        df = df.sort(['cnpj', 'cnpjcpf', 'data_bd'], ascending=[True, True, False])
        df = df.drop_duplicates(subset=['cnpj', 'cnpjcpf'], take_last=False)
        df = df.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
        del df['index']
        global header_id_carteira_fundos
        header_id_carteira_fundos = df.get_value(0, 'header_id').astype(str)

        # quadro de operaçoes
        # nese bloco há a criação de duas tabelas no banco de dados, onde conterão a lista de fundos de primeiro nivel da carteira da HDI
        # e também uma lista dos FIDCs na carteira da HDI
        # as variaveis lista_aux, lista_cnpj, lista_fidc e lista_aux_fidc participam do processo de criação dessas listas
        # Tais listas serao utilizadas nos códigos posteriores
        global lista_primeiro_nivel
        x = 'select a.* from projeto_inv.xml_quadro_operacoes a right join (select header_id, max(data_bd) as data_bd from projeto_inv.xml_quadro_operacoes where header_id=' + header_id_carteira_fundos + ' group by 1) b on a.header_id=b.header_id and a.data_bd=b.data_bd;'
        qo = pd.read_sql(x, con=connection)

        lista_primeiro_nivel = qo[['cnpjfundo_1nivel', 'fundo']]
        lista_primeiro_nivel = lista_primeiro_nivel.sort_values(by=['cnpjfundo_1nivel'], ascending=True)
        lista_primeiro_nivel = lista_primeiro_nivel.drop_duplicates(['cnpjfundo_1nivel'])
        lista_primeiro_nivel = lista_primeiro_nivel.reset_index(level=None, drop=False, inplace=False, col_level=0,
                                                                col_fill='')
        lista_primeiro_nivel = lista_primeiro_nivel[~(lista_primeiro_nivel['cnpjfundo_1nivel'].isnull())]
        global y
        y = 0
        i = 0
        lista_cnpj = pd.DataFrame(columns=['nome', 'cnpj', 'header_id', 'dt_ref', 'data_bd'])
        lista_fidc = pd.DataFrame(columns=['nome', 'cnpj', 'header_id', 'dt_ref', 'data_bd'])
        lista_aux = pd.DataFrame(None, index=[0], columns=['nome', 'cnpj', 'header_id', 'dt_ref', 'data_bd'])
        lista_aux_fidc = pd.DataFrame(None, index=[0], columns=['nome', 'cnpj', 'header_id', 'dt_ref', 'data_bd'])
        if len(lista_primeiro_nivel) > 0:
            for y in range(0, len(lista_primeiro_nivel)):
                cnpjlista = lista_primeiro_nivel['cnpjfundo_1nivel'][y]
                try:
                    parametrizacao(dt, cnpjlista)
                    lista_aux.set_value(lista_aux.index[0], 'cnpj', lista_primeiro_nivel['cnpjfundo_1nivel'][y])
                    lista_aux.set_value(lista_aux.index[0], 'nome', lista_primeiro_nivel['fundo'][y])
                    lista_cnpj = lista_cnpj.append(lista_aux)
                except:
                    print(lista_primeiro_nivel['cnpjfundo_1nivel'][y])
                    lista_aux_fidc.set_value(lista_aux.index[0], 'cnpj', lista_primeiro_nivel['cnpjfundo_1nivel'][y])
                    lista_aux_fidc.set_value(lista_aux.index[0], 'nome', lista_primeiro_nivel['fundo'][y])
                    lista_fidc = lista_fidc.append(lista_aux_fidc)

        lista_cnpj.loc[-2] = ['HDI Brasil', '29980158000157', None, None, None]
        lista_cnpj.loc[-1] = ['HDI Global', '18096627000153', None, None, None]
        lista_cnpj['dt_ref'] = dtbase_concat
        lista_cnpj['data_bd'] = datetime.datetime.now()
        lista_cnpj['header_id'] = header_id_carteira_fundos
        lista_cnpj = lista_cnpj.sort()
        lista_cnpj = lista_cnpj.reset_index(level=None, drop=False, inplace=False, col_level=0, col_fill='')
        del lista_cnpj['index']

        lista_fidc['dt_ref'] = dtbase_concat
        lista_fidc['data_bd'] = datetime.datetime.now()
        lista_fidc['header_id'] = header_id_carteira_fundos

        pd.io.sql.to_sql(lista_cnpj, name='lista_fundos', con=connection, if_exists="append", flavor='mysql', index=0)
        pd.io.sql.to_sql(lista_fidc, name='lista_fidc', con=connection, if_exists="append", flavor='mysql', index=0)

    import pandas as pd
    import pymysql as db
    import numpy as np
    import datetime
    from findt import FinDt
    from dependencias.Metodos.funcoes_auxiliares import get_data_ultimo_dia_util_mes_anterior
    from dependencias.Metodos.funcoes_auxiliares import full_path_from_database

    # Pega a data do último dia útil do mês anterior e deixa no formato específico para utilização da função
    dtbase = get_data_ultimo_dia_util_mes_anterior()
    dtbase_concat = dtbase[0] + dtbase[1] + dtbase[2]
    dt_base = dtbase[0]+'-'+dtbase[1]+'-'+dtbase[2]

    # Diretório de save de planilhas
    save_path_validacao_quadroconso = full_path_from_database('get_output_quadro419') + 'validacao_quadroconso.xlsx'

    # Diretório de dependencias
    depend_path_SPW_FI = full_path_from_database('excels') + 'SPW_FI.TXT'
    depend_path_fundos_estruturados = full_path_from_database('excels') + 'fundos_estruturados.xlsx'
    feriados_sheet = full_path_from_database('feriados_nacionais') + 'feriados_nacionais.csv'
    end_val = full_path_from_database('get_output_quadro419')

    connection = db.connect('localhost', user='root', passwd='root', db='projeto_inv', charset='utf8')

    query = 'select distinct nome_emissor, cnpj_emissor, data_criacao_emissor from projeto_inv.bmf_emissor where cnpj_emissor>0;'
    emissor = pd.read_sql(query, con=connection)
    emissor = emissor.sort(['cnpj_emissor', 'data_criacao_emissor'], ascending=[True, False])
    emissor1 = emissor.drop_duplicates(subset=['cnpj_emissor'], take_last=False)
    emissor1['cnpj'] = emissor1['cnpj_emissor'].astype(float)
    emissor1 = emissor1.rename(columns={'nome_emissor': 'contraparte'})
    emissor2 = emissor1[['cnpj', 'contraparte']]

    # gerar o arquivo consolidado
    parametrizacao(dt_base, cnpj)
    print("Fim parametrizacao")

    # gerar o arquivo para os fundos do primeiro nivel
    fundosprimeironivel(dt_base, cnpj)
    print("Fim fundosprimeironivel")

    connection.close()