package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import sys.dao.AlmacenDAO;
import sys.dao.AreaDAO;
import sys.dao.CentroCostoDAO;
import sys.dao.ClienteDAO;
import sys.dao.LocalidadDAO;
import sys.dao.NotaDespachoDAO;
import sys.dao.VendedorDAO;
import sys.model.AvmovGuiaRemisionDet;
import sys.model.AvmovMovNotaDespachoCab;
import sys.util.Service;

public class NotaDespachoDAOImp implements NotaDespachoDAO {

    @Override
    public List<AvmovMovNotaDespachoCab> listarNotaDespachos() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovMovNotaDespachoCab notaDespacho = null;
        List<AvmovMovNotaDespachoCab> lista = new ArrayList<>();

        ClienteDAO cDAO = new ClienteDAOImp();
        LocalidadDAO lDAO = new LocalidadDAOImp();
        CentroCostoDAO cCDAO = new CentroCostoDAOImp();
        AreaDAO areaDAO = new AreaDAOImp();
        AlmacenDAO almDAO = new AlmacenDAOImp();
        try {
            cn = Service.getConexion();
            String sql = "SELECT id_MovValeCab, ruc_companyia, cod_persona, "
                    + "num_vale, cod_establecimiento, cod_centroc, cod_area, "
                    + "cod_almacen, cod_campo, cod_sector, cod_tipoaplicacion, "
                    + "fec_vale, uso_equipos, fec_justificacion, num_movimiento, "
                    + "NomOperador, num_Area, num_Litrosxcilindro, num_LitrosSegunBidon, "
                    + "num_TancadasxBidon, num_LitrosxCilindros, num_GastoAgua, "
                    + "Exportado, Importado, fec_Importacion, hor_Importacion, "
                    + "cod_usuario_imp, Correlativo_Mov, flg_mochila, fec_creacion, "
                    + "hor_creacion, cod_usuario_creacion, fec_actualizacion, "
                    + "hor_actualizacion, cod_usuario_actualizacion, FLG_MOVSTOCK, "
                    + "FEC_MOVSTOCK, HOR_MOVSTOCK, cod_usuario_MovStock, "
                    + "cod_supervisor, cod_aplicador, id_periodoreingreso, "
                    + "flg_tipocalculo, ctd_cilindro, gasto_total, gasto_ha, "
                    + "flg_preAplicacion, est_preAplicacion, flg_tienepreAplicacion, "
                    + "flg_correlativoAplicacion, cod_campo_mostrar, des_campo_mostrar, "
                    + "COD_CONCEPTO, id_cultivo, Id_Cultivo_EstFenologico, cod_tipo_documento "
                    + "FROM AVMOV_MovNotaDespachoCab "
                    + "ORDER BY fec_vale DESC , num_vale DESC;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                existe = true;
                notaDespacho = new AvmovMovNotaDespachoCab();

                notaDespacho.setIdMovValeCab(rs.getInt("id_MovValeCab"));
                notaDespacho.setRucCompanyia(rs.getString("ruc_companyia"));
                notaDespacho.setCodPersona(rs.getInt("cod_persona"));
                notaDespacho.setNumVale(rs.getString("num_vale"));
                notaDespacho.setCodEstablecimiento(rs.getString("cod_establecimiento"));
                notaDespacho.setCodCentroc(rs.getInt("cod_centroc"));
                notaDespacho.setCodArea(rs.getInt("cod_area"));
                notaDespacho.setCodAlmacen(rs.getInt("cod_almacen"));
                notaDespacho.setCodCampo(rs.getInt("cod_campo"));
                notaDespacho.setCodSector(rs.getInt("cod_sector"));
                notaDespacho.setCodTipoaplicacion(rs.getInt("cod_tipoaplicacion"));
                notaDespacho.setFecVale(rs.getDate("fec_vale"));
                notaDespacho.setUsoEquipos(rs.getString("uso_equipos"));
                notaDespacho.setFecJustificacion(rs.getString("fec_justificacion"));
                notaDespacho.setNumMovimiento(rs.getString("num_movimiento"));
                notaDespacho.setNomOperador(rs.getString("NomOperador"));
                notaDespacho.setNumArea(rs.getInt("num_Area"));
                notaDespacho.setNumLitrosxcilindro(rs.getInt("num_Litrosxcilindro"));
                notaDespacho.setNumLitrosSegunBidon(rs.getInt("num_LitrosSegunBidon"));
                notaDespacho.setNumTancadasxBidon(rs.getInt("num_TancadasxBidon"));
                notaDespacho.setNumLitrosxCilindros(rs.getInt("num_LitrosxCilindros"));
                notaDespacho.setNumGastoAgua(rs.getInt("num_GastoAgua"));
                notaDespacho.setExportado(rs.getString("Exportado"));
                notaDespacho.setImportado(rs.getString("Importado"));
                notaDespacho.setFecImportacion(rs.getString("fec_Importacion"));
                notaDespacho.setHorImportacion(rs.getString("hor_Importacion"));
                notaDespacho.setCodUsuarioImp(rs.getInt("cod_usuario_imp"));
                notaDespacho.setCorrelativoMov(rs.getInt("Correlativo_Mov"));
                notaDespacho.setFlgMochila(rs.getString("flg_mochila"));
                notaDespacho.setFecCreacion(rs.getString("fec_creacion"));
                notaDespacho.setHorCreacion(rs.getString("hor_creacion"));
                notaDespacho.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                notaDespacho.setFecActualizacion(rs.getString("fec_actualizacion"));
                notaDespacho.setHorActualizacion(rs.getString("hor_actualizacion"));
                notaDespacho.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
                notaDespacho.setFlgMovstock(rs.getString("FLG_MOVSTOCK"));
                notaDespacho.setFecMovstock(rs.getString("FEC_MOVSTOCK"));
                notaDespacho.setHorMovstock(rs.getString("HOR_MOVSTOCK"));
                notaDespacho.setCodUsuarioMovStock(rs.getInt("cod_usuario_MovStock"));
                notaDespacho.setCodSupervisor(rs.getInt("cod_supervisor"));
                notaDespacho.setCodAplicador(rs.getInt("cod_aplicador"));
                notaDespacho.setIdPeriodoreingreso(rs.getInt("id_periodoreingreso"));
                notaDespacho.setFlgTipocalculo(rs.getInt("flg_tipocalculo"));
                notaDespacho.setCtdCilindro(rs.getInt("ctd_cilindro"));
                notaDespacho.setGastoTotal(rs.getInt("gasto_total"));
                notaDespacho.setGastoHa(rs.getInt("gasto_ha"));
                notaDespacho.setFlgPreAplicacion(rs.getString("flg_preAplicacion"));
                notaDespacho.setEstPreAplicacion(rs.getString("est_preAplicacion"));
                notaDespacho.setFlgTienepreAplicacion(rs.getString("flg_tienepreAplicacion"));
                notaDespacho.setFlgCorrelativoAplicacion(rs.getInt("flg_correlativoAplicacion"));
                notaDespacho.setCodCampoMostrar(rs.getInt("cod_campo_mostrar"));
                notaDespacho.setDesCampoMostrar(rs.getString("des_campo_mostrar"));
                notaDespacho.setCodConcepto(rs.getString("COD_CONCEPTO"));
                notaDespacho.setIdCultivo(rs.getInt("id_cultivo"));
                notaDespacho.setIdCultivoEstFenologico(rs.getInt("Id_Cultivo_EstFenologico"));
                notaDespacho.setCodTipoDocumento(rs.getString("cod_tipo_documento"));
                notaDespacho.setAgmaePersona(cDAO.consultarObjCliente(notaDespacho.getCodPersona()));

                notaDespacho.setAgmaeEstablecimiento(lDAO.consultarObjEstablecimiento(notaDespacho.getRucCompanyia(), notaDespacho.getCodEstablecimiento()));
                notaDespacho.setAgmaeCentrocosto(cCDAO.consultarObjCentroCosto(notaDespacho.getCodCentroc()));
                notaDespacho.setAgmaeArea(areaDAO.consultarObjArea(notaDespacho.getRucCompanyia(), notaDespacho.getCodArea()));
                notaDespacho.setAimarAlmacen(almDAO.consultarObjAlmacen(notaDespacho.getRucCompanyia(), notaDespacho.getCodAlmacen()));

                lista.add(notaDespacho);
            }
            if (!existe) {
                notaDespacho = null;
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
    public List<AvmovMovNotaDespachoCab> listaNotaDespachos(Date feDesde, Date feHasta, List<AvmovGuiaRemisionDet> listaGuiaRemisionDetalle) {
        String idNotaDespachoNoVa = "0";
        if (listaGuiaRemisionDetalle.size() > 0) {
            for (AvmovGuiaRemisionDet item : listaGuiaRemisionDetalle) {
                if (item.getCodTipoDocumentoOrigen().equals("09")) {
                    idNotaDespachoNoVa = idNotaDespachoNoVa + "," + item.getIdOrigen();
                }
            }
        }
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovMovNotaDespachoCab notaDespacho = null;
        List<AvmovMovNotaDespachoCab> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_MovNotaDespachoCab.id_MovValeCab AS idND, "
                    + "AVMOV_MovNotaDespachoCab.num_vale AS numND, "
                    + "AVMOV_MovNotaDespachoCab.fec_vale AS fecND, "
                    + "AVMOV_MovNotaDespachoCab.ruc_companyia AS rucCompanya, "
                    + "AVMOV_MovNotaDespachoCab.cod_persona AS codPersona, "
                    + "agmae_persona.Nom_Razon_Social AS nomPersona, "
                    + "agmae_persona.num_identificacion AS rucPersona, "
                    + "AVMOV_MovNotaDespachoCab.flg_estado AS flgEstado "
                    + "FROM (AVMOV_MovNotaDespachoCab LEFT JOIN agmae_persona ON AVMOV_MovNotaDespachoCab.cod_persona = agmae_persona.cod_persona) INNER JOIN AVMOV_MovNotaDespachoDet ON AVMOV_MovNotaDespachoCab.id_MovValeCab = AVMOV_MovNotaDespachoDet.id_MovValeCab "
                    + "WHERE (((AVMOV_MovNotaDespachoCab.flg_estado)<>'A') AND ((AVMOV_MovNotaDespachoDet.flg_estado_guia_remision)<>1) AND ((AVMOV_MovNotaDespachoCab.fec_vale)>=[?] AND (AVMOV_MovNotaDespachoCab.fec_vale)<=[?]) "
                    + "AND (AVMOV_MovNotaDespachoDet.id_MovValeProducto NOT IN(" + idNotaDespachoNoVa + ")) ) "
                    + "GROUP BY AVMOV_MovNotaDespachoCab.id_MovValeCab, AVMOV_MovNotaDespachoCab.num_vale, AVMOV_MovNotaDespachoCab.fec_vale, AVMOV_MovNotaDespachoCab.ruc_companyia, AVMOV_MovNotaDespachoCab.cod_persona, agmae_persona.Nom_Razon_Social, agmae_persona.num_identificacion, AVMOV_MovNotaDespachoCab.flg_estado; ";
            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(feDesde));
            ps.setDate(2, convertJavaDateToSqlDate(feHasta));

            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                notaDespacho = new AvmovMovNotaDespachoCab();

                notaDespacho.setIdMovValeCab(rs.getInt("idND"));
                notaDespacho.setNumVale(rs.getString("numND"));
                notaDespacho.setFecVale(rs.getDate("fecND"));
                notaDespacho.setRucCompanyia(rs.getString("rucCompanya"));
                notaDespacho.setCodPersona(rs.getInt("codPersona"));
                notaDespacho.setZRucPersona(rs.getString("rucPersona"));
                notaDespacho.setZNomPersona(rs.getString("nomPersona"));
                notaDespacho.setFlgEstado(rs.getString("flgEstado"));

                lista.add(notaDespacho);
            }
            if (!existe) {
                notaDespacho = null;
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
    public List<AvmovMovNotaDespachoCab> listarNotaDespachosPorFecha(Date feDesde, Date feHasta) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovMovNotaDespachoCab notaDespacho = null;
        List<AvmovMovNotaDespachoCab> lista = new ArrayList<>();

        try {

            cn = Service.getConexion();
            String sql = "SELECT AVMOV_MovNotaDespachoCab.id_MovValeCab AS idMov, AVMOV_MovNotaDespachoCab.fec_vale AS fecVale, AVMOV_MovNotaDespachoCab.num_vale AS numVale, AVMOV_MovNotaDespachoCab.des_observacion AS desND, AVMOV_MovNotaDespachoCab.cod_persona AS codPersona, agmae_persona.num_identificacion AS rucPersona, agmae_persona.Nom_Razon_Social AS nomPersona, AVMOV_MovNotaDespachoCab.ruc_companyia AS rucCompanya, agmae_companyias.des_companyia AS nomCompanya, AVMOV_MovNotaDespachoCab.cod_establecimiento AS codEstablecimiento, AGMAE_Establecimiento.NOM_establecimiento AS nomEstablecimiento, AVMOV_MovNotaDespachoCab.cod_centroc AS codCentro, agmae_centrocosto.des_centroc AS desCentro, AVMOV_MovNotaDespachoCab.cod_area AS codArea, AGMAE_Area.Des_Area AS desArea, AVMOV_MovNotaDespachoCab.cod_almacen AS codAlmacen, AIMAR_Almacen.Nom_Ai_Almacen AS nomAlmacen, AVMOV_MovNotaDespachoCab.cod_prioridad AS codPrioridad, ACMAE_Prioridad.des_prioridad AS nomProridad, AGMAE_ESTADO_TIPO_DOCUMENTO.des_estado AS nomEstado, AGMAE_ESTADO.des_estado AS estImprimir, AGMAE_ESTADO_1.des_estado AS estEditar, AGMAE_ESTADO_2.des_estado AS estAnular,  "
                    + "IIF((SELECT Count(ndd.id_MovValeProducto) AS Cantidad "
                    + "FROM AVMOV_MovNotaDespachoDet AS ndd "
                    + "WHERE (((ndd.flg_estado_factura)=0) AND ((ndd.id_MovValeCab)=AVMOV_MovNotaDespachoCab.id_MovValeCab)))>0 "
                    + ",'false','true') AS estFactura,  "
                    + "IIF((SELECT Count(ndd.id_MovValeProducto) AS Cantidad "
                    + "FROM AVMOV_MovNotaDespachoDet AS ndd "
                    + "WHERE (((ndd.flg_estado_guia_remision)=0) AND ((ndd.id_MovValeCab)=AVMOV_MovNotaDespachoCab.id_MovValeCab)))>0 "
                    + ",'false','true') AS estGuiaRemision  "
                    + "FROM AGMAE_ESTADO AS AGMAE_ESTADO_2 RIGHT JOIN (AGMAE_ESTADO AS AGMAE_ESTADO_1 RIGHT JOIN ((AGMAE_ESTADO_TIPO_DOCUMENTO RIGHT JOIN (((AIMAR_Almacen INNER JOIN (AGMAE_Area INNER JOIN ((AGMAE_Establecimiento INNER JOIN (AVMOV_MovNotaDespachoCab LEFT JOIN agmae_persona ON AVMOV_MovNotaDespachoCab.cod_persona = agmae_persona.cod_persona) ON (AGMAE_Establecimiento.COD_establecimiento = AVMOV_MovNotaDespachoCab.cod_establecimiento) AND (AGMAE_Establecimiento.ruc_companyia = AVMOV_MovNotaDespachoCab.ruc_companyia)) INNER JOIN agmae_centrocosto ON AVMOV_MovNotaDespachoCab.cod_centroc = agmae_centrocosto.cod_centroc) ON (AGMAE_Area.Cod_Area = AVMOV_MovNotaDespachoCab.cod_area) AND (AGMAE_Area.ruc_companyia = AVMOV_MovNotaDespachoCab.ruc_companyia)) ON (AIMAR_Almacen.Cod_Ai_Almacen = AVMOV_MovNotaDespachoCab.cod_almacen) AND (AIMAR_Almacen.ruc_companyia = AVMOV_MovNotaDespachoCab.ruc_companyia)) LEFT JOIN ACMAE_Prioridad ON AVMOV_MovNotaDespachoCab.cod_prioridad = ACMAE_Prioridad.cod_prioridad) INNER JOIN agmae_companyias ON AVMOV_MovNotaDespachoCab.ruc_companyia = agmae_companyias.ruc_companyia) ON (AGMAE_ESTADO_TIPO_DOCUMENTO.cod_documento = AVMOV_MovNotaDespachoCab.cod_tipo_documento) AND (AGMAE_ESTADO_TIPO_DOCUMENTO.cod_estado = AVMOV_MovNotaDespachoCab.flg_estado)) LEFT JOIN AGMAE_ESTADO ON AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_print = AGMAE_ESTADO.cod_estado) ON AGMAE_ESTADO_1.cod_estado = AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_edit) ON AGMAE_ESTADO_2.cod_estado = AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_del "
                    + "WHERE (((AVMOV_MovNotaDespachoCab.fec_vale)>=? And (AVMOV_MovNotaDespachoCab.fec_vale)<=?)) "
                    + "ORDER BY AVMOV_MovNotaDespachoCab.fec_vale DESC , AVMOV_MovNotaDespachoCab.num_vale DESC;";

            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(feDesde));
            ps.setDate(2, convertJavaDateToSqlDate(feHasta));

            rs = ps.executeQuery();

            while (rs.next()) {
                existe = true;
                notaDespacho = new AvmovMovNotaDespachoCab();

                notaDespacho.setIdMovValeCab(rs.getInt("idMov"));
                notaDespacho.setFecVale(rs.getDate("fecVale"));
                notaDespacho.setNumVale(rs.getString("numVale"));
                notaDespacho.setDesObservacion(rs.getString("desND"));
                notaDespacho.setCodPersona(rs.getInt("codPersona"));
                notaDespacho.setZRucPersona(rs.getString("rucPersona"));
                notaDespacho.setZNomPersona(rs.getString("nomPersona"));
                notaDespacho.setRucCompanyia(rs.getString("rucCompanya"));
                notaDespacho.setZNomCompanya(rs.getString("nomCompanya"));
                notaDespacho.setCodEstablecimiento(rs.getString("codEstablecimiento"));
                notaDespacho.setZNomEstablecimiento(rs.getString("nomEstablecimiento"));
                notaDespacho.setCodCentroc(rs.getInt("codCentro"));
                notaDespacho.setZNomCentro(rs.getString("desCentro"));
                notaDespacho.setCodArea(rs.getInt("codArea"));
                notaDespacho.setZNomArea(rs.getString("desArea"));
                notaDespacho.setCodAlmacen(rs.getInt("codAlmacen"));
                notaDespacho.setZNomAlmacen(rs.getString("nomAlmacen"));
                notaDespacho.setCodPrioridad(rs.getInt("codPrioridad"));
                notaDespacho.setZNomPrioridad(rs.getString("nomProridad"));
                notaDespacho.setZnomEstado(rs.getString("nomEstado"));
                notaDespacho.setZestadoImprimir(rs.getString("estImprimir"));
                notaDespacho.setZestadoEditar(rs.getString("estEditar"));
                notaDespacho.setZestadoAnular(rs.getString("estAnular"));
                
                notaDespacho.setZestadoFactura(rs.getString("estFactura"));
                notaDespacho.setZestadoGuiaRemision(rs.getString("estGuiaRemision"));

                lista.add(notaDespacho);
            }
            if (!existe) {
                notaDespacho = null;
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
    public List<AvmovMovNotaDespachoCab> listarNotaDespachosAFactura() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovMovNotaDespachoCab notaDespacho = null;
        List<AvmovMovNotaDespachoCab> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_MovNotaDespachoCab.id_MovValeCab AS idND, AVMOV_MovNotaDespachoCab.fec_vale AS fecVale, AVMOV_MovNotaDespachoCab.num_vale AS numVale, AVMOV_MovNotaDespachoCab.cod_persona AS codPersona, agmae_persona.num_identificacion AS rucPersona, agmae_persona.Nom_Razon_Social AS nomPersona, AVMOV_MovNotaDespachoDet.ruc_Companyia AS rucCompanya, agmae_companyias.des_companyia AS nomCompanya, AVMOV_MovNotaDespachoDet.flg_estado_factura AS codEstFactura,  IIf(AVMOV_MovNotaDespachoDet.flg_estado_factura=1,'true','false') AS nomEstFactura,  "
                    + "IIF(Count(AVMOV_MovNotaDespachoDet.flg_estado_factura)=IIf(AVMOV_MovNotaDespachoDet.flg_estado_factura=1 ,Count(AVMOV_MovNotaDespachoDet.flg_estado_factura),0),'FACTURADO', "
                    + "IIF(Count(AVMOV_MovNotaDespachoDet.flg_estado_factura)=IIf(AVMOV_MovNotaDespachoDet.flg_estado_factura=0 ,Count(AVMOV_MovNotaDespachoDet.flg_estado_factura),0),'PENDIENTE', 'PARCIAL') ) AS desEstadoFactura "
                    + "FROM ((AVMOV_MovNotaDespachoDet INNER JOIN AVMOV_MovNotaDespachoCab ON AVMOV_MovNotaDespachoDet.id_MovValeCab = AVMOV_MovNotaDespachoCab.id_MovValeCab) LEFT JOIN agmae_persona ON AVMOV_MovNotaDespachoCab.cod_persona = agmae_persona.cod_persona) LEFT JOIN agmae_companyias ON AVMOV_MovNotaDespachoDet.ruc_Companyia = agmae_companyias.ruc_companyia "
                    + "GROUP BY AVMOV_MovNotaDespachoCab.id_MovValeCab, AVMOV_MovNotaDespachoCab.fec_vale, AVMOV_MovNotaDespachoCab.num_vale, AVMOV_MovNotaDespachoCab.cod_persona, agmae_persona.num_identificacion, agmae_persona.Nom_Razon_Social, AVMOV_MovNotaDespachoDet.ruc_Companyia, agmae_companyias.des_companyia, AVMOV_MovNotaDespachoDet.flg_estado_factura, AVMOV_MovNotaDespachoCab.flg_estado_factura;";
            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                notaDespacho = new AvmovMovNotaDespachoCab();

                notaDespacho.setIdMovValeCab(rs.getInt("idND"));
                notaDespacho.setFecVale(rs.getDate("fecVale"));
                notaDespacho.setNumVale(rs.getString("numVale"));
                notaDespacho.setCodPersona(rs.getInt("codPersona"));
                notaDespacho.setZRucPersona(rs.getString("rucPersona"));
                notaDespacho.setZNomPersona(rs.getString("nomPersona"));
                notaDespacho.setRucCompanyia(rs.getString("rucCompanya"));
                notaDespacho.setZNomCompanya(rs.getString("nomCompanya"));
                notaDespacho.setFlgEstadoFactura(rs.getInt("codEstFactura"));
                notaDespacho.setZnomEstadoFactura(rs.getString("nomEstFactura"));
                notaDespacho.setZdesEstadoFactura(rs.getString("desEstadoFactura"));

                lista.add(notaDespacho);
            }
            if (!existe) {
                notaDespacho = null;
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
    public AvmovMovNotaDespachoCab obtenerNotaDespacho(AvmovMovNotaDespachoCab nd) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AvmovMovNotaDespachoCab notaDespacho = null;

        ClienteDAO cDAO = new ClienteDAOImp();
        LocalidadDAO lDAO = new LocalidadDAOImp();
        CentroCostoDAO cCDAO = new CentroCostoDAOImp();
        AreaDAO areaDAO = new AreaDAOImp();
        AlmacenDAO almDAO = new AlmacenDAOImp();
        VendedorDAO vDAO = new VendedorDAOImp();

        try {
            cn = Service.getConexion();
            String sql = "SELECT id_MovValeCab, ruc_companyia, cod_persona, "
                    + "num_vale, cod_establecimiento, cod_centroc, cod_area, "
                    + "cod_almacen, cod_campo, cod_sector, cod_tipoaplicacion, "
                    + "fec_vale, uso_equipos, fec_justificacion, num_movimiento, "
                    + "NomOperador, num_Area, num_Litrosxcilindro, num_LitrosSegunBidon, "
                    + "num_TancadasxBidon, num_LitrosxCilindros, num_GastoAgua, "
                    + "Exportado, Importado, fec_Importacion, hor_Importacion, "
                    + "cod_usuario_imp, Correlativo_Mov, flg_mochila, fec_creacion, "
                    + "hor_creacion, cod_usuario_creacion, fec_actualizacion, "
                    + "hor_actualizacion, cod_usuario_actualizacion, FLG_MOVSTOCK, "
                    + "FEC_MOVSTOCK, HOR_MOVSTOCK, cod_usuario_MovStock, "
                    + "cod_supervisor, cod_aplicador, id_periodoreingreso, "
                    + "flg_tipocalculo, ctd_cilindro, gasto_total, gasto_ha, "
                    + "flg_preAplicacion, est_preAplicacion, flg_tienepreAplicacion, "
                    + "flg_correlativoAplicacion, cod_campo_mostrar, des_campo_mostrar, "
                    + "COD_CONCEPTO, id_cultivo, Id_Cultivo_EstFenologico, flg_estado,cod_vendedor, des_observacion, cod_tipo_documento "
                    + "FROM AVMOV_MovNotaDespachoCab "
                    + "WHERE num_vale=?;";

            notaDespacho = new AvmovMovNotaDespachoCab();
            ps = cn.prepareStatement(sql);
            ps.setString(1, nd.getNumVale());

            rs = ps.executeQuery();

            if (rs.next()) {
                notaDespacho.setIdMovValeCab(rs.getInt("id_MovValeCab"));
                notaDespacho.setRucCompanyia(rs.getString("ruc_companyia"));
                notaDespacho.setCodPersona(rs.getInt("cod_persona"));
                notaDespacho.setNumVale(rs.getString("num_vale"));
                notaDespacho.setCodEstablecimiento(rs.getString("cod_establecimiento"));
                notaDespacho.setCodCentroc(rs.getInt("cod_centroc"));
                notaDespacho.setCodArea(rs.getInt("cod_area"));
                notaDespacho.setCodAlmacen(rs.getInt("cod_almacen"));
                notaDespacho.setCodCampo(rs.getInt("cod_campo"));
                notaDespacho.setCodSector(rs.getInt("cod_sector"));
                notaDespacho.setCodTipoaplicacion(rs.getInt("cod_tipoaplicacion"));
                notaDespacho.setFecVale(rs.getDate("fec_vale"));
                notaDespacho.setUsoEquipos(rs.getString("uso_equipos"));
                notaDespacho.setFecJustificacion(rs.getString("fec_justificacion"));
                notaDespacho.setNumMovimiento(rs.getString("num_movimiento"));
                notaDespacho.setNomOperador(rs.getString("NomOperador"));
                notaDespacho.setNumArea(rs.getInt("num_Area"));
                notaDespacho.setNumLitrosxcilindro(rs.getInt("num_Litrosxcilindro"));
                notaDespacho.setNumLitrosSegunBidon(rs.getInt("num_LitrosSegunBidon"));
                notaDespacho.setNumTancadasxBidon(rs.getInt("num_TancadasxBidon"));
                notaDespacho.setNumLitrosxCilindros(rs.getInt("num_LitrosxCilindros"));
                notaDespacho.setNumGastoAgua(rs.getInt("num_GastoAgua"));
                notaDespacho.setExportado(rs.getString("Exportado"));
                notaDespacho.setImportado(rs.getString("Importado"));
                notaDespacho.setFecImportacion(rs.getString("fec_Importacion"));
                notaDespacho.setHorImportacion(rs.getString("hor_Importacion"));
                notaDespacho.setCodUsuarioImp(rs.getInt("cod_usuario_imp"));
                notaDespacho.setCorrelativoMov(rs.getInt("Correlativo_Mov"));
                notaDespacho.setFlgMochila(rs.getString("flg_mochila"));
                notaDespacho.setFecCreacion(rs.getString("fec_creacion"));
                notaDespacho.setHorCreacion(rs.getString("hor_creacion"));
                notaDespacho.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                notaDespacho.setFecActualizacion(rs.getString("fec_actualizacion"));
                notaDespacho.setHorActualizacion(rs.getString("hor_actualizacion"));
                notaDespacho.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
                notaDespacho.setFlgMovstock(rs.getString("FLG_MOVSTOCK"));
                notaDespacho.setFecMovstock(rs.getString("FEC_MOVSTOCK"));
                notaDespacho.setHorMovstock(rs.getString("HOR_MOVSTOCK"));
                notaDespacho.setCodUsuarioMovStock(rs.getInt("cod_usuario_MovStock"));
                notaDespacho.setCodSupervisor(rs.getInt("cod_supervisor"));
                notaDespacho.setCodAplicador(rs.getInt("cod_aplicador"));
                notaDespacho.setIdPeriodoreingreso(rs.getInt("id_periodoreingreso"));
                notaDespacho.setFlgTipocalculo(rs.getInt("flg_tipocalculo"));
                notaDespacho.setCtdCilindro(rs.getInt("ctd_cilindro"));
                notaDespacho.setGastoTotal(rs.getInt("gasto_total"));
                notaDespacho.setGastoHa(rs.getInt("gasto_ha"));
                notaDespacho.setFlgPreAplicacion(rs.getString("flg_preAplicacion"));
                notaDespacho.setEstPreAplicacion(rs.getString("est_preAplicacion"));
                notaDespacho.setFlgTienepreAplicacion(rs.getString("flg_tienepreAplicacion"));
                notaDespacho.setFlgCorrelativoAplicacion(rs.getInt("flg_correlativoAplicacion"));
                notaDespacho.setCodCampoMostrar(rs.getInt("cod_campo_mostrar"));
                notaDespacho.setDesCampoMostrar(rs.getString("des_campo_mostrar"));
                notaDespacho.setCodConcepto(rs.getString("COD_CONCEPTO"));
                notaDespacho.setIdCultivo(rs.getInt("id_cultivo"));
                notaDespacho.setIdCultivoEstFenologico(rs.getInt("Id_Cultivo_EstFenologico"));
                notaDespacho.setFlgEstado(rs.getString("flg_estado"));
                notaDespacho.setCodVendedor(rs.getInt("cod_vendedor"));
                notaDespacho.setDesObservacion(rs.getString("des_observacion"));
                notaDespacho.setCodTipoDocumento(rs.getString("cod_tipo_documento"));

                notaDespacho.setAgmaePersona(cDAO.consultarObjCliente(notaDespacho.getCodPersona()));

                notaDespacho.setAgmaeEstablecimiento(lDAO.consultarObjEstablecimiento(notaDespacho.getRucCompanyia(), notaDespacho.getCodEstablecimiento()));
                notaDespacho.setAgmaeCentrocosto(cCDAO.consultarObjCentroCosto(notaDespacho.getCodCentroc()));
                notaDespacho.setAgmaeArea(areaDAO.consultarObjArea(notaDespacho.getRucCompanyia(), notaDespacho.getCodArea()));
                notaDespacho.setAimarAlmacen(almDAO.consultarObjAlmacen(notaDespacho.getRucCompanyia(), notaDespacho.getCodAlmacen()));
                notaDespacho.setAgmaeVendedor(vDAO.consultarObjVendedor(notaDespacho.getCodVendedor()));
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
        return notaDespacho;
    }

    @Override
    public AvmovMovNotaDespachoCab obtenerIdNotaDespacho(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT id_MovValeCab "
                    + "FROM AVMOV_MovNotaDespachoCab "
                    + "WHERE num_vale=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, notaDespacho.getNumVale());

            rs = ps.executeQuery();

            if (rs.next()) {
                notaDespacho.setIdMovValeCab(rs.getInt("id_MovValeCab"));
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
        return notaDespacho;
    }

    @Override
    public void newNotaDespacho(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AVMOV_MovNotaDespachoCab (`ruc_companyia`, "
                + "`fec_vale`, `num_vale`, `cod_establecimiento`, `cod_centroc`,"
                + "`cod_area`, `cod_almacen`,`des_observacion`, `fec_creacion`, `hor_creacion`,"
                + " `cod_usuario_creacion`,`flg_estado`,`cod_persona`,`cod_vendedor`,`cod_tipo_documento`) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespacho.setFecCreacion(formatoFecha.format(fechaActual));
        notaDespacho.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, notaDespacho.getRucCompanyia());
            ps.setDate(2, convertJavaDateToSqlDate(notaDespacho.getFecVale()));
            ps.setString(3, notaDespacho.getNumVale());
            ps.setString(4, notaDespacho.getCodEstablecimiento());
            ps.setInt(5, notaDespacho.getCodCentroc());
            ps.setInt(6, notaDespacho.getCodArea());
            ps.setInt(7, notaDespacho.getCodAlmacen());
            ps.setString(8, notaDespacho.getDesObservacion());
            ps.setString(9, notaDespacho.getFecCreacion());
            ps.setString(10, notaDespacho.getHorCreacion());
            ps.setInt(11, notaDespacho.getCodUsuarioCreacion());
            ps.setString(12, notaDespacho.getFlgEstado());
            ps.setInt(13, notaDespacho.getCodPersona());
            ps.setInt(14, notaDespacho.getCodVendedor());
            ps.setString(15, notaDespacho.getCodTipoDocumento());
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
    }

    @Override
    public void updateNotaDespacho(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AVMOV_MovNotaDespachoCab SET "
                + "ruc_companyia=?, \n"
                + "cod_persona=?, \n"
                + "cod_establecimiento=?, \n"
                + "cod_centroc=?, \n"
                + "cod_area=?, \n"
                + "cod_almacen=?, \n"
                + "cod_campo=?, \n"
                + "cod_sector=?, \n"
                + "cod_tipoaplicacion=?, \n"
                + "fec_vale=?, \n"
                + "uso_equipos=?, \n"
                + "fec_justificacion=?, \n"
                + "num_movimiento=?, \n"
                + "NomOperador=?, \n"
                + "num_Area=?, \n"
                + "num_Litrosxcilindro=?, \n"
                + "num_LitrosSegunBidon=?, \n"
                + "num_TancadasxBidon=?, \n"
                + "num_LitrosxCilindros=?, \n"
                + "num_GastoAgua=?, \n"
                + "Exportado=?, \n"
                + "Importado=?, \n"
                + "fec_Importacion=?, \n"
                + "hor_Importacion=?, \n"
                + "cod_usuario_imp=?, \n"
                + "Correlativo_Mov=?, \n"
                + "flg_mochila=?, \n"
                + "fec_actualizacion=?, \n"
                + "hor_actualizacion=?, \n"
                + "cod_usuario_actualizacion=?, \n"
                + "FLG_MOVSTOCK=?, \n"
                + "FEC_MOVSTOCK=?, \n"
                + "HOR_MOVSTOCK=?, \n"
                + "cod_usuario_MovStock=?, \n"
                + "cod_supervisor=?, \n"
                + "cod_aplicador=?, \n"
                + "id_periodoreingreso=?, \n"
                + "flg_tipocalculo=?, \n"
                + "ctd_cilindro=?, \n"
                + "gasto_total=?, \n"
                + "gasto_ha=?, \n"
                + "flg_preAplicacion=?, \n"
                + "est_preAplicacion=?, \n"
                + "flg_tienepreAplicacion=?, \n"
                + "flg_correlativoAplicacion=?, \n"
                + "cod_campo_mostrar=?, \n"
                + "des_campo_mostrar=?, \n"
                + "COD_CONCEPTO=?, \n"
                + "id_cultivo=?, \n"
                + "Id_Cultivo_EstFenologico=?, \n"
                + "cod_vendedor=?, \n"
                + "cod_prioridad=?, \n"
                + "des_observacion=?, \n"
                + "flg_estado=? "
                + "WHERE num_vale=?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespacho.setFecActualizacion(formatoFecha.format(fechaActual));
        notaDespacho.setHorActualizacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, notaDespacho.getRucCompanyia());
            ps.setInt(2, notaDespacho.getCodPersona());
            ps.setString(3, notaDespacho.getCodEstablecimiento());
            ps.setInt(4, notaDespacho.getCodCentroc());
            ps.setInt(5, notaDespacho.getCodArea());
            ps.setInt(6, notaDespacho.getCodAlmacen());
            ps.setInt(7, notaDespacho.getCodCampo());
            ps.setInt(8, notaDespacho.getCodSector());
            ps.setInt(9, notaDespacho.getCodTipoaplicacion());
            ps.setDate(10, convertJavaDateToSqlDate(notaDespacho.getFecVale()));
            ps.setString(11, notaDespacho.getUsoEquipos());
            ps.setString(12, notaDespacho.getFecJustificacion());
            ps.setString(13, notaDespacho.getNumMovimiento());
            ps.setString(14, notaDespacho.getNomOperador());
            ps.setInt(15, notaDespacho.getNumArea());
            ps.setInt(16, notaDespacho.getNumLitrosxcilindro());
            ps.setInt(17, notaDespacho.getNumLitrosSegunBidon());
            ps.setInt(18, notaDespacho.getNumTancadasxBidon());
            ps.setInt(19, notaDespacho.getNumLitrosxCilindros());
            ps.setInt(20, notaDespacho.getNumGastoAgua());
            ps.setString(21, notaDespacho.getExportado());
            ps.setString(22, notaDespacho.getImportado());
            ps.setString(23, notaDespacho.getFecImportacion());
            ps.setString(24, notaDespacho.getHorImportacion());
            ps.setInt(25, notaDespacho.getCodUsuarioImp());
            ps.setInt(26, notaDespacho.getCorrelativoMov());
            ps.setString(27, notaDespacho.getFlgMochila());
            ps.setString(28, notaDespacho.getFecActualizacion());
            ps.setString(29, notaDespacho.getHorActualizacion());
            ps.setInt(30, notaDespacho.getCodUsuarioActualizacion());
            ps.setString(31, notaDespacho.getFlgMovstock());
            ps.setString(32, notaDespacho.getFecMovstock());
            ps.setString(33, notaDespacho.getHorMovstock());
            ps.setInt(34, notaDespacho.getCodUsuarioMovStock());
            ps.setInt(35, notaDespacho.getCodSupervisor());
            ps.setInt(36, notaDespacho.getCodAplicador());
            ps.setInt(37, notaDespacho.getIdPeriodoreingreso());
            ps.setInt(38, notaDespacho.getFlgTipocalculo());
            ps.setInt(39, notaDespacho.getCtdCilindro());
            ps.setInt(40, notaDespacho.getGastoTotal());
            ps.setInt(41, notaDespacho.getGastoHa());
            ps.setString(42, notaDespacho.getFlgPreAplicacion());
            ps.setString(43, notaDespacho.getEstPreAplicacion());
            ps.setString(44, notaDespacho.getFlgTienepreAplicacion());
            ps.setInt(45, notaDespacho.getFlgCorrelativoAplicacion());
            ps.setInt(46, notaDespacho.getCodCampoMostrar());
            ps.setString(47, notaDespacho.getDesCampoMostrar());
            ps.setString(48, notaDespacho.getCodConcepto());
            ps.setInt(49, notaDespacho.getIdCultivo());
            ps.setInt(50, notaDespacho.getIdCultivoEstFenologico());
            ps.setInt(51, notaDespacho.getCodVendedor());
            ps.setInt(52, notaDespacho.getCodPrioridad());
            ps.setString(53, notaDespacho.getDesObservacion());
            ps.setString(54, notaDespacho.getFlgEstado());
            ps.setString(55, notaDespacho.getNumVale());
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

    public java.sql.Date convertJavaDateToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }
}
