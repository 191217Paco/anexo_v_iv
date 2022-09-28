using PLADSE.Servicios.API.RENAPO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Anexo5_6
{
    public static class FiltradoCheq
    {
        public static (int num_regis, string mensaje)  AcoplamientoSentencias(int qna_proc, int cons_qna_proc)
        {
            DataTable dtHcc = new DataTable();
            DataTable dtNt = new DataTable();
            DataTable dtE = new DataTable();
            DataTable dtEC = new DataTable();
            DataTable dtEnss = new DataTable();
            DataTable dtEec = new DataTable();
            string mensaje = "importado";

            DataTable ask = EjecutarSentencia("select * from anexo_v_pnr where qna_proc = "+qna_proc+" and cons_qna_proc = "+cons_qna_proc+"", "timbradoestatal");
            if (ask != null )
            {
                if ( ask.Rows.Count == 0)
                {
                    string queryNT = "SELECT idnomina, qna_proc, cons_qna_proc from nominas_timbrado where qna_proc = " + qna_proc + " and cons_qna_proc =  " + cons_qna_proc + "";
                    dtNt = EjecutarSentencia(queryNT, "timbradoestatal");

                    string queryHccNt = "SELECT '" + dtNt.Rows[0]["idnomina"] + "' as idnomina, HCC.num_cheque as no_comprobante, ur = 'R07',HCC.qna_proc, HCC.rfc, CASE WHEN HCC.cons_qna_proc > 0 THEN 'E' ELSE 'O' END as t_nomina, ((((right('00' + CONVERT([varchar](2), cod_pago, (0)), (2)) " +
                                            "+ right('00' + CONVERT([varchar](2), unidad, (0)), (2))) +right('00' + CONVERT([varchar](2), SubUnidad, (0)), (2))) " +
                                            "+right('' + CONVERT([varchar](7), replace(cat_Puesto, ' ', ''), (0)),(7))) +right('000' + format(horas * 10, 'f0'), (3))) " +
                                            "+right('000000' + CONVERT([varchar](6), cons_Plaza, (0)),(6)) as plaza, HCC.qna_pago as fecha_pago , " +
                                            "HCC.qna_ini as fecha_inicio, HCC.qna_fin as fecha_termino, HCC.tot_perc_cheque as percepciones, " +
                                            "HCC.tot_ded_cheque as deducciones, tot_neto_cheque as neto, " +
                                            "(((right('00' + CONVERT([varchar](2), HCC.ent_fed, (0)), (2)) + CONVERT([varchar](4), HCC.ct_clasif, (0))) + CONVERT([varchar](4)," +
                                            "HCC.ct_id,(0)))+right('0000' + CONVERT([varchar](4), HCC.ct_secuencial, (0)),(4)))+HCC.ct_digito_ver as 'cct', " +
                                            "HCC.tipo_pago as forma_pago, cve_banco = '', HCC.mot_mov as motivo, nivel_cm = 'I', HCC.qna_proc, HCC.num_cheque, HCC.cheque_dv, " +
                                            "grupo = 'OT' + (right('00' + CONVERT([varchar](2), HCC.cons_qna_proc, (0)), (2))) , HCC.cons_qna_proc, * " +
                                            "FROM hist_cheque_cpto_c0 as HCC " +
                                            "WHERE (HCC.qna_proc = " + dtNt.Rows[0]["qna_proc"] + ") and(HCC.cons_qna_proc = " + dtNt.Rows[0]["cons_qna_proc"] + ")";

                    dtHcc = EjecutarSentencia(queryHccNt, "hsiapsep");


                    foreach (DataRow rowHcc in dtHcc.Rows)
                    {                      
                        string queryRfcs_E = "select e.rfc, e.nombre as nombres, e.paterno as primer_apellido, e.materno as segundo_apellido " +
                                             "from empleado e " +
                                             "where '" + rowHcc["rfc"] + "' = e.rfc";

                        dtE = EjecutarSentencia(queryRfcs_E, "siapsep");
                        if (dtE.Rows.Count == 0)
                        {
                            string queryRfcs_Ps = "select distinct ps.rfc_sust, ps.nombre_sust as nombres, ps.paterno_sust as primer_apellido, ps.materno_sust as segundo_apellido from pagos_sustitutos ps where '" + rowHcc["rfc"] + "' = ps.rfc_sust    ";
                            dtE = EjecutarSentencia(queryRfcs_Ps, "fup");

                        }                    

                        if (dtE.Rows.Count >= 1)
                        {
                            var tupla = FindCurp(dtEC, rowHcc);

                            dtEC = tupla.dtEC;
                            rowHcc["rfc"] = tupla.rowHcc["rfc"];                        
                            Boolean result = CurpValida(dtEC.Rows[0]["curp"].ToString());

                            if (!result)
                            {

                                string queryInsertInvalidCurp = "IF NOT EXISTS (SELECT * FROM EXCEPCIONES_RFC_CURP WHERE rfc = '" + dtEC.Rows[0]["rfc"] + "') BEGIN " +
                                    "INSERT INTO [dbo].[EXCEPCIONES_RFC_CURP]([idexcepcion],[RFC],[CURP],[nombre],[qna_proc],[cons_qna_proc]) " +
                                    "VALUES( newid(),'" + dtEC.Rows[0]["rfc"] + "','" + dtEC.Rows[0]["curp"] + "','" + dtE.Rows[0]["nombres"] + "'," + dtHcc.Rows[0]["qna_proc"] + "," + dtHcc.Rows[0]["cons_qna_proc"] + ") END";
                                EjecutarSentencia(queryInsertInvalidCurp, "consultalectura");
                            }

                            string queryRfcs_Enss = "select enss.rfc, enss.numero_nss as nss from empleado_nss as enss where '" + rowHcc["rfc"] + "' = enss.rfc";
                            dtEnss = EjecutarSentencia(queryRfcs_Enss, "siapsep");

                            if (dtEnss.Rows.Count > 0)
                            {
                                foreach (DataRow rowEnss in dtEnss.Rows)
                                {                                    
                                    if (rowEnss["nss"].ToString().Length == 1)
                                    {                                        
                                        rowEnss[1] = "";
                                    }                                                                        
                                }
                            }
                            else
                            {
                                dtEnss.Columns.Add(new DataColumn("rfc")); rowHcc["rfc"].ToString();
                                dtEnss.Columns.Add(new DataColumn("nss"));
                                DataRow dtr = dtEnss.NewRow();
                                dtr["rfc"] = rowHcc["rfc"];
                                dtr["nss"] = "";
                                dtEnss.Rows.Add(dtr);

                            }                            
                            string periodo = AcomodaPeriodo(rowHcc["qna_proc"].ToString());
                            string queryAnexo_V = "INSERT INTO [dbo].[anexo_v_pnr]" +
                                "([idanexo_v_pnr],[idnomina],[no_comprobante],[ur],[periodo],[tipo_nomina],[primer_apellido],[segundo_apellido],[nombres]," +
                                "[clave_plaza],[curp],[rfc],[fecha_pago],[fecha_inicio],[fecha_termino],[percepciones],[deducciones],[neto],[nss],[cct]," +
                                "[forma_pago],[cve_banco],[clabe],[motivo],[nivel_cm],[qna_proc],[cons_qna_proc],[num_cheque],[cheque_dv],[grupo])" +
                                "VALUES( newid(),'" + rowHcc["idnomina"].ToString() + "','" + rowHcc["no_comprobante"].ToString() + "','" + rowHcc["ur"].ToString() + "', '" + periodo + "', '" + rowHcc["t_nomina"].ToString() + "','" + dtE.Rows[0]["primer_apellido"].ToString() + "','" + dtE.Rows[0]["segundo_apellido"].ToString() + "', " +
                                "'" + dtE.Rows[0]["nombres"].ToString() + "','" + rowHcc["plaza"].ToString() + "','" + dtEC.Rows[0]["curp"].ToString() + "','" + rowHcc["rfc"].ToString() + "','" + QuincenaToFecha2(Int32.Parse(rowHcc["fecha_pago"].ToString()), rowHcc["t_nomina"].ToString()) + "'," +
                                "'" + QuincenaToFecha2(Int32.Parse(rowHcc["fecha_inicio"].ToString()),"A") + "','" + QuincenaToFecha2(Int32.Parse(rowHcc["fecha_termino"].ToString()), "B") + "'," + rowHcc["percepciones"] + "," + rowHcc["deducciones"].ToString() + "," + rowHcc["neto"].ToString() +
                                ",'" + dtEnss.Rows[0]["nss"].ToString() + "','" + rowHcc["cct"].ToString() + "','" + rowHcc["forma_pago"].ToString() + "','','', '" + rowHcc["motivo"].ToString() + "','" + rowHcc["nivel_cm"].ToString() +
                                "'," + rowHcc["qna_proc"].ToString() + "," + rowHcc["cons_qna_proc"].ToString() + "," + rowHcc["num_cheque"].ToString() + ",'" + rowHcc["cheque_dv"].ToString() + "','" + rowHcc["grupo"].ToString() + "')";                            
                            EjecutarSentencia(queryAnexo_V, "timbradoestatal");
                            
                            int iterar = int.Parse(rowHcc["num_perc"].ToString()) + int.Parse(rowHcc["num_desc"].ToString());
                            for (int i = 1; i <= iterar; i++)
                            {

                                string cifra = "" + i;
                                string cifra2 = "" + cifra.PadLeft(2, '0');
                                string queryCpto = " SELECT perc_ded, concepto, descripcion FROM ptda_concepto WHERE concepto = '" + rowHcc["concepto" + cifra2] + "'";

                                DataTable dtCpto = EjecutarSentencia(queryCpto, "siapsep");
                                string queryInsA6 = "INSERT INTO [dbo].[anexo_vi_pnr] ([idanexo_vi_pnr],[idnomina],[no_comprobante],[ur],[periodo],[tipo_nomina], [clave_plaza],[curp],[tipo_concepto],[cod_concepto],[desc_concepto]," +
                                    "[importe],[base_calculo_isr],[observaciones],[conciliaciones],[ministracion],[consecutivo],[qna_proc],[cons_qna_proc],[grupo]) " +
                                    "VALUES(newid(),'" + rowHcc["idnomina"].ToString() + "','" + rowHcc["no_comprobante"].ToString() + "','" + rowHcc["ur"].ToString() + "','" + periodo + "','" + rowHcc["t_nomina"].ToString() + "','" + rowHcc["plaza"].ToString() + "','" + dtEC.Rows[0]["curp"].ToString() + "'," +
                                    "'" + dtCpto.Rows[0]["perc_ded"] + "','" + dtCpto.Rows[0]["concepto"] + "','" + dtCpto.Rows[0]["descripcion"] + "'," + rowHcc["importe" + cifra2] + ",'" + Cpto_gravables(dtCpto.Rows[0]["concepto"].ToString()) + "','','',null," +
                                    "null," + rowHcc["qna_proc"] + "," + rowHcc["cons_qna_proc"] + ",'" + rowHcc["grupo"].ToString() + "')";                                
                                EjecutarSentencia(queryInsA6, "timbradoestatal");
                            }
                        }
                    }
                }
                else
                {
                    mensaje = "existentes";
                }
            }
            else
            {
                mensaje = "existentes";
            }
            return (dtHcc.Rows.Count, mensaje);
        }


        private static int Cpto_gravables(string cod_concepto)
        {
            int base_calculo_isr = 0;
            DataTable ref_x_cpto = null/* TODO Change to default(_) if this is not a reference type */;
            char[] arraycptoAPQ = new char[] { 'A', 'P', 'Q' };
            List<string> Ax = new List<string>();
            foreach (char element in arraycptoAPQ)
            {
                for (int x = 65; x <= 90; x++)
                    Ax.Add(string.Format("{0}{1}", element, Convert.ToChar(x)));
            }

            char[] arraycptoAQ = new char[] { 'A', 'Q' };
            // Dim Ax As List(Of String) = New List(Of String)
            foreach (char element in arraycptoAQ)
            {
                for (int x = 1; x <= 5; x++)
                    Ax.Add(string.Format("{0}{1}", element, x));
            }
            //ref_x_cpto = dbfone.ExtraeTabla("select * from ref_x_cptos where perc_ded='D' and concepto='01' and qna_fin=999999 and concepto_rel not like \"%n\" and concepto_rel not like \"%x\"");
            ref_x_cpto = EjecutarSentencia("select * from ref_x_cptos where perc_ded='D' and concepto='01' and qna_fin=999999 and concepto_rel not like '%n' and concepto_rel not like '%x'", "siapsep");
            if (ref_x_cpto != null & ref_x_cpto != null)
            {
                if (ref_x_cpto.Rows.Count > 0)
                {
                    foreach (DataRow cpto in ref_x_cpto.Rows)
                        Ax.Add(cpto["concepto_rel"].ToString());
                }
            }
            Ax.Add("63B");
            Ax.Add("65B");
            Ax.Add("66B");
            Ax.Add("67B");
            Ax.Add("69B");
            if ((Ax.IndexOf(cod_concepto.ToString().TrimEnd(' ')) > -1))
            {
                base_calculo_isr = 1;
            }
            return base_calculo_isr;
        }


        private static (DataTable dtEC, DataRow rowHcc) FindCurp(DataTable dtEC, DataRow rowHcc)
        {
            //Console.WriteLine("Epa si fuiomos invocados");
            //Console.WriteLine("rfc "+rowHcc["rfc"]);
            string queryRfcs_Ec = "select ec.rfc, ec.cve_unica as curp from empleado_curp as ec where '" + rowHcc["rfc"] + "' = ec.rfc";
            dtEC = EjecutarSentencia(queryRfcs_Ec, "siapsep");
            //Console.WriteLine(dtEC.Rows.Count);
            if (dtEC.Rows.Count == 0)
            {
                //Console.WriteLine("Epaaaa no encotramos nada en empleado_curp");
                string queryRfcs_Eec = "select pec.rfc, pec.cve_unica as curp from pagos_especiales_curp as pec where '" + rowHcc["rfc"] + "' = pec.rfc";
                dtEC = EjecutarSentencia(queryRfcs_Eec, "fup");
                if (dtEC.Rows.Count == 0)
                {
                    //Console.WriteLine("Epaaaaaaaaaaaaaaa tampoco encontramos en pagos especiales");
                    DataTable dtTemp = EjecutarSentencia("select rfc_nvo from cambio_rfc where rfc_anterior = '" + rowHcc["rfc"] + "'", "siapsep");
                    if (dtTemp.Rows.Count >= 1)
                    {
                        //Console.WriteLine("rfc actualizado : " + dtTemp.Rows[0][0].ToString());
                        rowHcc["rfc"] = dtTemp.Rows[0][0].ToString();

                        var tpl = FindCurp(dtEC, rowHcc);
                        dtEC = tpl.dtEC;
                        rowHcc = tpl.rowHcc;



                    }

                }

            }

            return (dtEC, rowHcc);
        }


        private static string AcomodaPeriodo(string str)
        {
            string years = str.Substring(0, 4);
            string qna = str.Substring(4, 2);
            //Console.WriteLine("periodo " + str);
            //Console.WriteLine("años " + years);
            //Console.WriteLine("quincena " + qna);

            string periodo = qna +"/"+ years;
            //Console.WriteLine("periodoOK " + periodo);
            return periodo;
        }

        private static Boolean CurpValida(String CURP)
        {
            //Console.WriteLine("Creo que vmaos bien");
            if (CURP.Length != 18)
                return false;
            ConsultaCURP consultaCURP = new ConsultaCURP();
            try
            {
                ResponseRenapo responseRenapo = consultaCURP.ConsultaPorCurp(CURP);
                if (responseRenapo.ConsultaExitosa == false)
                {
                    //Console.WriteLine("-----------------------Curp Invalida : " + CURP);

                    return false;
                }
                else
                {
                    //Console.WriteLine("Curp validad");
                    return true;
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetType().Name + ":" + ex.Message);
                return false;
            }


        }


        private static string QuincenaToFecha(Int32 anioquincena)
        {
            int qna = anioquincena % 100;
            int anio = System.Convert.ToInt32(anioquincena) / 100;
            int mes = System.Convert.ToInt32(qna / 2) + qna % 2;
            int dia = qna % 2 == 0 ? DateTime.DaysInMonth(anio, mes) : 15;


            string result = String.Format("{0}/{1}/{2}", dia, mes, anio);


            DateTime dateTime = DateTime.Parse(result);

            DateTime date = dateTime.Date;
            result = date.ToString("d");            
            return result;
        }

        private static string QuincenaToFecha2(Int32 yearqna, string tipo)
        {
            int qna;
            int anio;
            int mes;
            int dia;
            string fecha;
            if(Convert.ToString(yearqna).Length != 6)
            {
                fecha = null;
            }else
            {
                if (tipo == "B" & yearqna == 999999)
                {
                    fecha = "31/12/99999";
                }
                else
                {
                    if (int.Parse(Convert.ToString(yearqna).Substring(4, 2)) <1  || int.Parse(Convert.ToString(yearqna).Substring(4, 2)) > 24)
                    {
                        fecha = null;
                    }
                    else
                    {
                        qna = yearqna % 100;
                        anio = System.Convert.ToInt32(yearqna) / 100;
                        mes = System.Convert.ToInt32(qna / 2) + qna % 2;

                        if (tipo == "A")
                        {
                            if (qna % 2 == 0){dia = 16;}
                            else{dia = 1;}
                        }
                        else
                        {
                            if (qna % 2 == 0){dia = DateTime.DaysInMonth(anio, mes);}
                            else{dia = 15;}
                        }
                        fecha = String.Format("{0}/{1}/{2}", dia, mes, anio);                        
                        DateTime dateTime = DateTime.Parse(fecha);
                        DateTime date = dateTime.Date;
                        fecha = date.ToString("d");
                    }
                }
            }
            return fecha;
        }


        private static DataTable EjecutarSentencia(string sentencia, string baseD)
        {
            DataTable tabla = new DataTable();
            StringBuilder errorMessages = new StringBuilder();
            try
            {
                Conexion conn = new Conexion();
                conn.Coneccion(baseD);
                conn.GetConnection().Open();
                //conn.getCommand().CommandTimeout = 6000;
                conn.Ejecutar(sentencia);
                SqlDataReader dr = conn.GetCommand().ExecuteReader();
                if (dr.HasRows)
                {
                    try
                    {
                        tabla.Load(dr);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.GetType().Name + ":" + ex.Message);
                    }
                    dr.Close();
                    conn.GetConnection().Close();
                    conn.GetCommand().Dispose();
                    return tabla;
                }
                return tabla;
            }
            catch (SqlException ex)
            {
                for (int i = 0; i < ex.Errors.Count; i++)
                {

                    errorMessages.Append("Index #" + i + "\n" +
                        "Message ES: " + ex.Errors[i].Message + "\n" +
                        "Error Number ES: " + ex.Errors[i].Number + "\n" +
                        "LineNumber ES: " + ex.Errors[i].LineNumber + "\n" +
                        "Source ES: " + ex.Errors[i].Source + "\n" +
                        "Procedure ES: " + ex.Errors[i].Procedure + "\n"+
                        "SENTENCIA EJECUTADA : " + sentencia + "\n");
                }
                Console.WriteLine(errorMessages.ToString());
                return null;
            }
        }




    }

    class Conexion
    {
        private SqlConnection connection = new SqlConnection();
        private SqlCommand command = new SqlCommand();
        private readonly Dictionary<string, string> DiccionaryConnection = new Dictionary<string, string>()
            {
                {"consultalectura","data source=winsql;initial catalog=consultalectura;user id=udiaz;password=servicio2022!"},
                {"siapsep","data source=winsql;initial catalog=siapsep;user id=consultatimbrado;password=6A7F1A56-7252-4587-BBCD-236142240B50"},
                {"hsiapsep","data source=winsql;initial catalog=hsiapsep;user id=consultatimbrado;password=6A7F1A56-7252-4587-BBCD-236142240B50"},
                {"fup","data source=winsql;initial catalog=fup;user id=consultatimbrado;password=6A7F1A56-7252-4587-BBCD-236142240B50"},
                {"timbradoestatal","data source=winsql;initial catalog=TimbradoEstatal;user id=timbradoestatal;password=D312F891-ECB8-4A08-8A4B-DFB7D1ABCADE"}
            };

        public SqlConnection Coneccion(string baseD)
        {
            
            StringBuilder errorMessages = new StringBuilder();
            try
            {     
                connection = new SqlConnection(DiccionaryConnection[baseD]);
            }
            catch (SqlException ex)
            {
                for (int i = 0; i < ex.Errors.Count; i++)
                {
                    errorMessages.Append("Index #" + i + "\n" +
                        "Message: " + ex.Errors[i].Message + "\n" +
                        "Error Number: " + ex.Errors[i].Number + "\n" +
                        "LineNumber: " + ex.Errors[i].LineNumber + "\n" +
                        "Source: " + ex.Errors[i].Source + "\n" +
                        "Procedure: " + ex.Errors[i].Procedure + "\n");
                }
                Console.WriteLine(errorMessages.ToString());
            }

            return connection;
        }
        public void Ejecutar(string sentencia)
        {            
            command = new SqlCommand(sentencia, connection);
        }

        public SqlConnection GetConnection() { return connection; }

        public SqlCommand GetCommand() { return command; }

        public void ConnOpen()
        {
            StringBuilder errorMessages = new StringBuilder();
            try
            {
                connection.Open();
            }
            catch (SqlException ex)
            {
                for (int i = 0; i < ex.Errors.Count; i++)
                {
                    errorMessages.Append("Index #" + i + "\n" +
                        "Message: " + ex.Errors[i].Message + "\n" +
                        "Error Number: " + ex.Errors[i].Number + "\n" +
                        "LineNumber: " + ex.Errors[i].LineNumber + "\n" +
                        "Source: " + ex.Errors[i].Source + "\n" +
                        "Procedure: " + ex.Errors[i].Procedure + "\n");
                }
                Console.WriteLine(errorMessages.ToString());
            }

        }
        public void ConnClose()
        {
            connection.Close();
        }
    }

    
}
