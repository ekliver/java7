package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import sys.model.AvmovGuiaRemisionCab;
import sys.util.Service;
import sys.dao.GuiaRemisionDAO;
import sys.model.AvmovFacturaNdCab;
import sys.model.AvmovGuiaRemisionDet;

public class GuiaRemisionDAOImp implements GuiaRemisionDAO {

    @Override
    public int obtenerIdGuiaRemisionPorNumGuiaRemision(AvmovGuiaRemisionCab guiaRemsion) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int idGuiaRemision = 0;
        try {
            cn = Service.getConexion();
            String sql = "SELECT TOP 1 Id_guia_remision_cab "
                    + "FROM AVMOV_GUIA_REMISION_CAB "
                    + "WHERE nro_guia=? AND nro_serie=? "
                    + "ORDER BY 1 DESC; ";

            ps = cn.prepareStatement(sql);
            ps.setString(1, guiaRemsion.getNumGuia());
            ps.setString(2, guiaRemsion.getNumSerie());

            rs = ps.executeQuery();

            if (rs.next()) {
                idGuiaRemision = rs.getInt("Id_guia_remision_cab");

            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return idGuiaRemision;
    }

    @Override
    public AvmovGuiaRemisionCab obtenerGuiaRemisionPorIdGuiaRemision(AvmovGuiaRemisionCab gr) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AvmovGuiaRemisionCab guiaRemision = null;
        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_GUIA_REMISION_CAB.Id_guia_remision_cab AS idGR,"
                    + "AVMOV_GUIA_REMISION_CAB.nro_serie AS nroSerie,"
                    + "AVMOV_GUIA_REMISION_CAB.nro_guia AS nroGuia,"
                    + "AVMOV_GUIA_REMISION_CAB.ruc_companyia AS rucCompanyia,"
                    + "agmae_companyias.des_companyia AS nomCompanyia,"
                    + "AVMOV_GUIA_REMISION_CAB.des_punto_partida AS desPartida,"
                    + "AVMOV_GUIA_REMISION_CAB.fec_emision AS fecEmision,"
                    + "AVMOV_GUIA_REMISION_CAB.fec_inicio_traslado AS fecTraslado,"
                    + "AVMOV_GUIA_REMISION_CAB.num_costo_minimo AS numCosto,"
                    + "AVMOV_GUIA_REMISION_CAB.des_punto_llegada AS desLLegada,"
                    + "AVMOV_GUIA_REMISION_CAB.cod_persona AS codPersona,"
                    + "agmae_persona.num_identificacion AS rucPersona,"
                    + "agmae_persona.Nom_Razon_Social AS nomPersona,"
                    + "AVMOV_GUIA_REMISION_CAB.num_placa AS numPlaca,"
                    + "AIMAR_Unidad_Transporte.Mar_udt AS nomMarca,"
                    + "AIMAR_Unidad_Transporte.Nco_udt AS nomConstancia,"
                    + "AVMOV_GUIA_REMISION_CAB.dni_conductor AS dniConductor,"
                    + "AIMAR_Conductores.Des_con AS nomConductor,"
                    + "AIMAR_Conductores.Bre_con AS licConductor,"
                    + "AVMOV_GUIA_REMISION_CAB.cod_emptransporte AS codEmptransporte,"
                    + "AIMAR_EMPRESA_TRANSPORTE.ruc_emptransporte AS rucEmptransporte,"
                    + "AIMAR_EMPRESA_TRANSPORTE.nom_emptransporte AS nomEmptransporte,"
                    + "AIMAR_EMPRESA_TRANSPORTE.num_telefono AS tlfEmptransporte,"
                    + "AVMOV_GUIA_REMISION_CAB.num_cantidad_productos AS ctdProducto,"
                    + "AVMOV_GUIA_REMISION_CAB.cod_motivo AS codMotivo,"
                    + "AVMOV_GUIA_REMISION_CAB.cod_tipo_documento AS codTipoDocumento,"
                    + "AVMOV_GUIA_REMISION_CAB.flg_estado AS flgEstado,"
                    + "AVMOV_GUIA_REMISION_CAB.fec_creacion AS fecCreacion,"
                    + "AVMOV_GUIA_REMISION_CAB.hor_creacion AS horCreacion,"
                    + "AVMOV_GUIA_REMISION_CAB.cod_usuario_creacion AS usuCreacion,"
                    + "AVMOV_GUIA_REMISION_CAB.fec_actualizacion AS fecActualiza,"
                    + "AVMOV_GUIA_REMISION_CAB.hor_actualizacion AS horActualiza,"
                    + "AVMOV_GUIA_REMISION_CAB.cod_usuario_actualizacion AS usuActualiza "
                    + "FROM (AIMAR_Conductores RIGHT JOIN (AIMAR_Unidad_Transporte "
                    + "RIGHT JOIN ((AVMOV_GUIA_REMISION_CAB LEFT JOIN agmae_persona "
                    + "ON AVMOV_GUIA_REMISION_CAB.cod_persona = agmae_persona.cod_persona) "
                    + "LEFT JOIN agmae_companyias ON AVMOV_GUIA_REMISION_CAB.ruc_companyia = agmae_companyias.ruc_companyia) "
                    + "ON AIMAR_Unidad_Transporte.Pla_udt = AVMOV_GUIA_REMISION_CAB.num_placa) "
                    + "ON AIMAR_Conductores.Dni_con = AVMOV_GUIA_REMISION_CAB.dni_conductor) "
                    + "LEFT JOIN AIMAR_EMPRESA_TRANSPORTE ON AVMOV_GUIA_REMISION_CAB.cod_emptransporte = AIMAR_EMPRESA_TRANSPORTE.cod_emptransporte "
                    + "WHERE AVMOV_GUIA_REMISION_CAB.Id_guia_remision_cab=? ; ";

            ps = cn.prepareStatement(sql);
            ps.setInt(1, gr.getIdGuiaRemisionCab());

            rs = ps.executeQuery();

            if (rs.next()) {
                guiaRemision = new AvmovGuiaRemisionCab();

                guiaRemision.setIdGuiaRemisionCab(rs.getInt("idGR"));
                guiaRemision.setNumSerie(rs.getString("nroSerie"));
                guiaRemision.setNumGuia(rs.getString("nroGuia"));
                guiaRemision.setRucCompanyia(rs.getString("rucCompanyia"));
                guiaRemision.setZnomCompanyia(rs.getString("nomCompanyia"));
                guiaRemision.setDesPuntoPartida(rs.getString("desPartida"));
                guiaRemision.setFecEmision(rs.getDate("fecEmision"));
                guiaRemision.setFecInicioTraslado(rs.getDate("fecTraslado"));
                guiaRemision.setNumCostoMinimo(rs.getInt("numCosto"));
                guiaRemision.setDesPuntoLlegada(rs.getString("desLLegada"));
                guiaRemision.setCodPersona(rs.getInt("codPersona"));
                guiaRemision.setZrucPersona(rs.getString("rucPersona"));
                guiaRemision.setZnomPersona(rs.getString("nomPersona"));
                guiaRemision.setNumPlaca(rs.getString("numPlaca"));
                guiaRemision.setZnomMarca(rs.getString("nomMarca"));
                guiaRemision.setZnomConstancia(rs.getString("nomConstancia"));
                guiaRemision.setDniConductor(rs.getString("dniConductor"));
                guiaRemision.setZnomConductor(rs.getString("nomConductor"));
                guiaRemision.setZlicConductor(rs.getString("licConductor"));
                guiaRemision.setCodEmptransporte(rs.getInt("codEmptransporte"));
                guiaRemision.setZrucEmpresa(rs.getString("rucEmptransporte"));
                guiaRemision.setZnomEmpresa(rs.getString("nomEmptransporte"));
                guiaRemision.setZnumTelefono(rs.getString("tlfEmptransporte"));
                guiaRemision.setNumCantidadProductos(rs.getInt("ctdProducto"));
                guiaRemision.setCodMotivo(rs.getInt("codMotivo"));
                guiaRemision.setCodTipoDocumento(rs.getString("codTipoDocumento"));
                guiaRemision.setFlgEstado(rs.getString("flgEstado"));
                guiaRemision.setFecCreacion(rs.getString("fecCreacion"));
                guiaRemision.setHorCreacion(rs.getString("horCreacion"));
                guiaRemision.setCodUsuarioCreacion(rs.getInt("usuCreacion"));
                guiaRemision.setFecActualizacion(rs.getString("fecActualiza"));
                guiaRemision.setHorActualizacion(rs.getString("horActualiza"));
                guiaRemision.setCodUsuarioActualizacion(rs.getInt("usuActualiza"));

            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return guiaRemision;
    }

    @Override
    public List<AvmovGuiaRemisionCab> listarGuiasRemisionPorFecha(Date feDesde, Date feHasta) {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovGuiaRemisionCab guiaRemision = null;
        List<AvmovGuiaRemisionCab> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_GUIA_REMISION_CAB.Id_guia_remision_cab AS idGR, "
                    + "AVMOV_GUIA_REMISION_CAB.fec_emision AS fecEmision,"
                    + "AVMOV_GUIA_REMISION_CAB.nro_serie AS nroSerie,"
                    + "AVMOV_GUIA_REMISION_CAB.nro_guia AS nroGuia,"
                    + "AVMOV_GUIA_REMISION_CAB.cod_persona AS codPersona,"
                    + "agmae_persona.num_identificacion AS rucPersona,"
                    + "agmae_persona.Nom_Razon_Social AS nomPersona,"
                    + "AVMOV_GUIA_REMISION_CAB.fec_inicio_traslado AS fecTraslado,"
                    + "AVMOV_GUIA_REMISION_CAB.ruc_companyia AS rucCompanyia,"
                    + "agmae_companyias.des_companyia AS nomCompanyia,"
                    + "AGMAE_ESTADO_TIPO_DOCUMENTO.des_estado AS estGR,"
                    + "AGMAE_ESTADO.des_estado AS estPrint,"
                    + "AGMAE_ESTADO_1.des_estado AS estEdit, "
                    + "AGMAE_ESTADO_2.des_estado AS estDel "
                    + "FROM AGMAE_ESTADO AS AGMAE_ESTADO_2 RIGHT JOIN (AGMAE_ESTADO AS AGMAE_ESTADO_1 "
                    + "RIGHT JOIN (AGMAE_ESTADO RIGHT JOIN (AGMAE_ESTADO_TIPO_DOCUMENTO RIGHT JOIN ((AVMOV_GUIA_REMISION_CAB LEFT JOIN agmae_persona ON AVMOV_GUIA_REMISION_CAB.cod_persona = agmae_persona.cod_persona) "
                    + "LEFT JOIN agmae_companyias ON AVMOV_GUIA_REMISION_CAB.ruc_companyia = agmae_companyias.ruc_companyia) ON (AGMAE_ESTADO_TIPO_DOCUMENTO.cod_estado = AVMOV_GUIA_REMISION_CAB.flg_estado) "
                    + "AND (AGMAE_ESTADO_TIPO_DOCUMENTO.cod_documento = AVMOV_GUIA_REMISION_CAB.cod_tipo_documento)) ON AGMAE_ESTADO.cod_estado = AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_print) ON AGMAE_ESTADO_1.cod_estado = AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_edit) ON AGMAE_ESTADO_2.cod_estado = AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_del "
                    + "WHERE (((AVMOV_GUIA_REMISION_CAB.fec_emision)>=? And (AVMOV_GUIA_REMISION_CAB.fec_emision)<=?))  "
                    + ";";
            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(feDesde));
            ps.setDate(2, convertJavaDateToSqlDate(feHasta));
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                guiaRemision = new AvmovGuiaRemisionCab();
                guiaRemision.setIdGuiaRemisionCab(rs.getInt("idGR"));
                guiaRemision.setFecEmision(rs.getDate("fecEmision"));
                guiaRemision.setNumSerie(rs.getString("nroSerie"));
                guiaRemision.setNumGuia(rs.getString("nroGuia"));
                guiaRemision.setCodPersona(rs.getInt("codPersona"));
                guiaRemision.setZrucPersona(rs.getString("rucPersona"));
                guiaRemision.setZnomPersona(rs.getString("nomPersona"));
                guiaRemision.setFecInicioTraslado(rs.getDate("fecTraslado"));
                guiaRemision.setRucCompanyia(rs.getString("rucCompanyia"));
                guiaRemision.setZnomCompanyia(rs.getString("nomCompanyia"));
                guiaRemision.setFlgEstado(rs.getString("estGR"));
                guiaRemision.setZestadoImprimir(rs.getString("estPrint"));
                guiaRemision.setZestadoEditar(rs.getString("estEdit"));
                guiaRemision.setZestadoAnular(rs.getString("estDel"));
                lista.add(guiaRemision);
            }
            if (!existe) {
                guiaRemision = null;
                System.out.println("No se encontro datos");
            }
//              Service.cerrarConexion();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return lista;

    }

    @Override
    public List<AvmovFacturaNdCab> listaFacturas(Date feDesde, Date feHasta, List<AvmovGuiaRemisionDet> listaGuiaRemisionDetalle) {
        String idFacturaNoVa = "0";
        String idNotaDespachoNoVa = "0";
        if (listaGuiaRemisionDetalle.size() > 0 && listaGuiaRemisionDetalle != null) {
            for (AvmovGuiaRemisionDet item : listaGuiaRemisionDetalle) {
                if (item.getCodTipoDocumentoOrigen().equals("01")) {
                    idFacturaNoVa = idFacturaNoVa + "," + item.getIdOrigen();
                } else if (item.getCodTipoDocumentoOrigen().equals("09")) {
                    idNotaDespachoNoVa = idNotaDespachoNoVa + "," + item.getIdOrigen();
                }
            }
        }
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovFacturaNdCab factura = null;
        List<AvmovFacturaNdCab> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_Factura_ND_CAB.idFacturaCab AS idFac, "
                    + "AVMOV_Factura_ND_CAB.num_serie AS numSerie, "
                    + "AVMOV_Factura_ND_CAB.num_factura AS numFac, "
                    + "AVMOV_Factura_ND_CAB.fec_factura AS fecFac, "
                    + "AVMOV_Factura_ND_CAB.idNotaDespachoCab AS idND, "
                    + "AVMOV_Factura_ND_CAB.cod_persona AS codPersona, "
                    + "agmae_persona.num_identificacion AS rucPersona, "
                    + "AVMOV_Factura_ND_CAB.ruc_companyia AS rucCompanya, "
                    + "agmae_persona.Nom_Razon_Social AS nomPersona "
                    + "FROM (AVMOV_Factura_ND_CAB LEFT JOIN agmae_persona ON AVMOV_Factura_ND_CAB.cod_persona = agmae_persona.cod_persona) INNER JOIN AVMOV_Factura_ND_DET ON AVMOV_Factura_ND_CAB.idFacturaCab = AVMOV_Factura_ND_DET.idFacturaCab "
                    + "WHERE (((AVMOV_Factura_ND_DET.flg_estado_guia_remision)<>1) AND ((AVMOV_Factura_ND_CAB.flg_estado)<>'A') "
                    + "AND ((AVMOV_Factura_ND_CAB.fec_factura)>=? And (AVMOV_Factura_ND_CAB.fec_factura)<=?) "
                    + "AND (AVMOV_Factura_ND_DET.idFacturaDet NOT IN(" + idFacturaNoVa + ")) "
                    + "AND (AVMOV_Factura_ND_DET.idNotaDespachoDet NOT IN(" + idNotaDespachoNoVa + "))) "
                    + "GROUP BY AVMOV_Factura_ND_CAB.idFacturaCab, AVMOV_Factura_ND_CAB.num_serie, AVMOV_Factura_ND_CAB.num_factura, "
                    + "AVMOV_Factura_ND_CAB.fec_factura, AVMOV_Factura_ND_CAB.idNotaDespachoCab, AVMOV_Factura_ND_CAB.cod_persona, "
                    + "agmae_persona.num_identificacion, AVMOV_Factura_ND_CAB.ruc_companyia, agmae_persona.Nom_Razon_Social, "
                    + "AVMOV_Factura_ND_DET.flg_estado_guia_remision, AVMOV_Factura_ND_CAB.flg_estado; ";

            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(feDesde));
            ps.setDate(2, convertJavaDateToSqlDate(feHasta));
            rs = ps.executeQuery();

            while (rs.next()) {
                existe = true;
                factura = new AvmovFacturaNdCab();

                factura.setIdFacturaCab(rs.getInt("idFac"));
                factura.setNumSerie(rs.getString("numSerie"));
                factura.setNumFactura(rs.getString("numFac"));
                factura.setFecFactura(rs.getDate("fecFac"));
                factura.setIdNotaDespachoCab(rs.getInt("idND"));
                factura.setRucCompanyia(rs.getString("rucCompanya"));
                factura.setCodPersona(rs.getInt("codPersona"));
                factura.setZrucPersona(rs.getString("rucPersona"));
                factura.setZnomPersona(rs.getString("nomPersona"));

                lista.add(factura);
            }
            if (!existe) {
                factura = null;
                System.out.println("No se encontro datos");
            }
//              Service.cerrarConexion();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return lista;

    }

    public java.sql.Date convertJavaDateToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }

    @Override
    public String generarNroGuiaRemision(AvmovGuiaRemisionCab guiaRemision) {
        String numDocumento = "";
        String cesDocumento = "";
        numDocumento = obtenerNroGuiaRemision(guiaRemision);
        if (numDocumento.equals("")) {
            newNroSerieGuiaRemision(guiaRemision);
        }
        newNroGuiaRemision(guiaRemision);

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT Agmae_numerador_documentos.num_documento, "
                    + "Agmae_numerador_documentos.ces_documento "
                    + "FROM Agmae_numerador_documentos "
                    + "WHERE (((Agmae_numerador_documentos.ruc_companyia)=[?]) "
                    + "AND ((Agmae_numerador_documentos.cod_documento)='09') "
                    + "AND ((Agmae_numerador_documentos.num_serie_documento)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setString(1, guiaRemision.getRucCompanyia());
            ps.setString(2, guiaRemision.getNumSerie());
            rs = ps.executeQuery();

            if (rs.next()) {
                numDocumento = String.valueOf(rs.getInt(1));
                cesDocumento = rs.getString(2);
                for (int i = 1; numDocumento.length() <= 4; i++) {
                    numDocumento = "0" + numDocumento;
                }
                numDocumento = cesDocumento + numDocumento;
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return numDocumento;
    }

    public String obtenerNroGuiaRemision(AvmovGuiaRemisionCab guiaRemision) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String numDocumento = "";
        try {
            cn = Service.getConexion();
            String sql = "SELECT Agmae_numerador_documentos.num_documento "
                    + "FROM Agmae_numerador_documentos "
                    + "WHERE (((Agmae_numerador_documentos.ruc_companyia)=[?]) "
                    + "AND ((Agmae_numerador_documentos.cod_documento)='09') "
                    + "AND ((Agmae_numerador_documentos.num_serie_documento)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setString(1, guiaRemision.getRucCompanyia());
            ps.setString(2, guiaRemision.getNumSerie());
            rs = ps.executeQuery();

            if (rs.next()) {
                numDocumento = String.valueOf(rs.getInt(1));

            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return numDocumento;
    }

    public void newNroGuiaRemision(AvmovGuiaRemisionCab guiaRemision) {
        Connection cn = null;
        PreparedStatement ps = null;
        try {
            cn = Service.getConexion();
            String sql = "UPDATE Agmae_numerador_documentos SET"
                    + " num_documento=num_documento+1 "
                    + "WHERE (((ruc_companyia)=[?]) "
                    + "AND ((cod_documento)='09') "
                    + "AND ((num_serie_documento)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setString(1, guiaRemision.getRucCompanyia());
            ps.setString(2, guiaRemision.getNumSerie());
            ps.executeUpdate();

//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    public void newNroSerieGuiaRemision(AvmovGuiaRemisionCab guiaRemision) {
        Connection cn = null;
        PreparedStatement ps = null;

        try {
            cn = Service.getConexion();
            String sql = "INSERT INTO Agmae_numerador_documentos(num_documento,ces_documento, "
                    + "cod_documento,ruc_companyia,num_serie_documento) "
                    + "VALUES(0,'G','09',?,?);";

            ps = cn.prepareStatement(sql);
            ps.setString(1, guiaRemision.getRucCompanyia());
            ps.setString(2, guiaRemision.getNumSerie());
            ps.executeUpdate();

//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

    }

    @Override
    public void newGuiaRemision(AvmovGuiaRemisionCab guiaRemision) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AVMOV_GUIA_REMISION_CAB ("
                + "`nro_serie`, \n"
                + "`nro_guia`, \n"
                + "`ruc_companyia`, \n"
                + "`des_punto_partida`, \n"
                + "`fec_emision`, \n"
                + "`fec_inicio_traslado`, \n"
                + "`num_costo_minimo`, \n"
                + "`des_punto_llegada`, \n"
                + "`cod_persona`, \n"
                + "`num_placa`, \n"
                + "`dni_conductor`, \n"
                + "`cod_emptransporte`, \n"
                + "`num_cantidad_productos`, \n"
                + "`cod_motivo`, \n"
                + "`cod_tipo_documento`, \n"
                + "`flg_estado`, \n"
                + "`fec_creacion`, \n"
                + "`hor_creacion`, \n"
                + "`cod_usuario_creacion` "
                + ") "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); ";
        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");
        guiaRemision.setFecCreacion(formatoFecha.format(fechaActual));
        guiaRemision.setHorCreacion(formatoHora.format(fechaActual));
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, guiaRemision.getNumSerie());
            ps.setString(2, guiaRemision.getNumGuia());
            ps.setString(3, guiaRemision.getRucCompanyia());
            ps.setString(4, guiaRemision.getDesPuntoPartida());
            ps.setDate(5, convertJavaDateToSqlDate(guiaRemision.getFecEmision()));
            ps.setDate(6, convertJavaDateToSqlDate(guiaRemision.getFecInicioTraslado()));
            ps.setDouble(7, guiaRemision.getNumCostoMinimo());
            ps.setString(8, guiaRemision.getDesPuntoLlegada());
            ps.setInt(9, guiaRemision.getCodPersona());
            ps.setString(10, guiaRemision.getNumPlaca());
            ps.setString(11, guiaRemision.getDniConductor());
            ps.setInt(12, guiaRemision.getCodEmptransporte());
            ps.setInt(13, guiaRemision.getNumCantidadProductos());
            ps.setInt(14, guiaRemision.getCodMotivo());
            ps.setString(15, guiaRemision.getCodTipoDocumento());
            ps.setString(16, guiaRemision.getFlgEstado());
            ps.setString(17, guiaRemision.getFecCreacion());
            ps.setString(18, guiaRemision.getHorCreacion());
            ps.setInt(19, guiaRemision.getCodUsuarioCreacion());

            ps.executeUpdate();
//              Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        //
    }

    @Override
    public void updateGuiaRemision(AvmovGuiaRemisionCab guiaRemision) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AVMOV_GUIA_REMISION_CAB SET "
                + "nro_serie=?, \n"
                + "nro_guia=?, \n"
                + "ruc_companyia=?, \n"
                + "des_punto_partida=?, \n"
                + "fec_emision=?, \n"
                + "fec_inicio_traslado=?, \n"
                + "num_costo_minimo=?, \n"
                + "des_punto_llegada=?, \n"
                + "cod_persona=?, \n"
                + "num_placa=?, \n"
                + "dni_conductor=?, \n"
                + "cod_emptransporte=?, \n"
                + "num_cantidad_productos=?, \n"
                + "cod_motivo=?, \n"
                + "cod_tipo_documento=?, \n"
                + "flg_estado=?, \n"
                + "fec_actualizacion=?, \n"
                + "hor_actualizacion=?, \n"
                + "cod_usuario_actualizacion=? "
                + "WHERE Id_guia_remision_cab= ?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        guiaRemision.setFecActualizacion(formatoFecha.format(fechaActual));
        guiaRemision.setHorActualizacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, guiaRemision.getNumSerie());
            ps.setString(2, guiaRemision.getNumGuia());
            ps.setString(3, guiaRemision.getRucCompanyia());
            ps.setString(4, guiaRemision.getDesPuntoPartida());
            ps.setDate(5, convertJavaDateToSqlDate(guiaRemision.getFecEmision()));
            ps.setDate(6, convertJavaDateToSqlDate(guiaRemision.getFecInicioTraslado()));
            ps.setDouble(7, guiaRemision.getNumCostoMinimo());
            ps.setString(8, guiaRemision.getDesPuntoLlegada());
            ps.setInt(9, guiaRemision.getCodPersona());
            ps.setString(10, guiaRemision.getNumPlaca());
            ps.setString(11, guiaRemision.getDniConductor());
            ps.setInt(12, guiaRemision.getCodEmptransporte());
            ps.setInt(13, guiaRemision.getNumCantidadProductos());
            ps.setInt(14, guiaRemision.getCodMotivo());
            ps.setString(15, guiaRemision.getCodTipoDocumento());
            ps.setString(16, guiaRemision.getFlgEstado());
            ps.setString(17, guiaRemision.getFecActualizacion());
            ps.setString(18, guiaRemision.getHorActualizacion());
            ps.setInt(19, guiaRemision.getCodUsuarioActualizacion());
            ps.setInt(20, guiaRemision.getIdGuiaRemisionCab());

            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

    }

    @Override
    public void deleteGuiaRemision(AvmovGuiaRemisionCab guiaRemision) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AVMOV_GUIA_REMISION_CAB WHERE Id_guia_remision_cab=?;";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, guiaRemision.getIdGuiaRemisionCab());
            ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

    }

}
