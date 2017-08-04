package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import sys.dao.AlmacenDAO;
import sys.dao.AreaDAO;
import sys.dao.CentroCostoDAO;
import sys.dao.NotaDespachoDetalleDAO;
import sys.dao.ProductoDAO;
import sys.model.AvmovGuiaRemisionDet;
import sys.model.AvmovMovNotaDespachoCab;
import sys.model.AvmovMovNotaDespachoDet;
import sys.util.Service;

public class NotaDespachoDetalleDAOImp implements NotaDespachoDetalleDAO {

    @Override
    public List<AvmovMovNotaDespachoDet> listarNotaDespachoDetalles() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovMovNotaDespachoDet notaDespachoDetalle = null;
        List<AvmovMovNotaDespachoDet> lista = new ArrayList<>();
        ProductoDAO pDAO = new ProductoDAOImp();

        CentroCostoDAO cCDAO = new CentroCostoDAOImp();
        AreaDAO areaDAO = new AreaDAOImp();
        AlmacenDAO almDAO = new AlmacenDAOImp();

        try {
            cn = Service.getConexion();
            String sql = "SELECT id_MovValeProducto, id_MovValeCab, ruc_Companyia, "
                    + "num_vale, cod_establecimiento, cod_centroc, cod_area, "
                    + "cod_almacen, cod_campo, cod_sector, cod_producto, ctd_dosis, "
                    + "num_tancadas, num_has, ctd_total, ctd_Movimiento, pre_vta, "
                    + "imp_vta, val_vta_sol, imp_vta_sol, val_vta_dol, imp_vta_dol, "
                    + "Exportado, Importado, fec_Importacion, hor_Importacion, "
                    + "cod_usuario_imp, fec_creacion, hor_creacion, cod_usuario_creacion, "
                    + "fec_actualizacion, hor_actualizacion, cod_usuario_actualizacion "
                    + "FROM AVMOV_MovNotaDespachoDet;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                notaDespachoDetalle = new AvmovMovNotaDespachoDet();

                notaDespachoDetalle.setIdMovValeProducto(rs.getInt("id_MovValeProducto"));
                notaDespachoDetalle.setIdMovValeCab(rs.getInt("id_MovValeCab"));
                notaDespachoDetalle.setRucCompanyia(rs.getString("ruc_Companyia"));
                notaDespachoDetalle.setNumVale(rs.getString("num_vale"));
                notaDespachoDetalle.setCodEstablecimiento(rs.getString("cod_establecimiento"));
                notaDespachoDetalle.setCodCentroc(rs.getInt("cod_centroc"));
                notaDespachoDetalle.setCodArea(rs.getInt("cod_area"));
                notaDespachoDetalle.setCodAlmacen(rs.getInt("cod_almacen"));
                notaDespachoDetalle.setCodCampo(rs.getInt("cod_campo"));
                notaDespachoDetalle.setCodSector(rs.getInt("cod_sector"));
                notaDespachoDetalle.setCodProducto(rs.getString("cod_producto"));
                notaDespachoDetalle.setCtdDosis(rs.getString("ctd_dosis"));
                notaDespachoDetalle.setNumTancadas(rs.getString("num_tancadas"));
                notaDespachoDetalle.setNumHas(rs.getInt("num_has"));
                notaDespachoDetalle.setCtdTotal(rs.getInt("ctd_total"));
                notaDespachoDetalle.setCtdMovimiento(rs.getInt("ctd_Movimiento"));
                notaDespachoDetalle.setPreVta(rs.getInt("pre_vta"));
                notaDespachoDetalle.setImpVta(rs.getInt("imp_vta"));
                notaDespachoDetalle.setValVtaSol(rs.getInt("val_vta_sol"));
                notaDespachoDetalle.setImpVtaSol(rs.getInt("imp_vta_sol"));
                notaDespachoDetalle.setValVtaDol(rs.getInt("val_vta_dol"));
                notaDespachoDetalle.setImpVtaDol(rs.getInt("imp_vta_dol"));
                notaDespachoDetalle.setExportado(rs.getString("Exportado"));
                notaDespachoDetalle.setImportado(rs.getString("Importado"));
                notaDespachoDetalle.setFecImportacion(rs.getString("fec_Importacion"));
                notaDespachoDetalle.setHorImportacion(rs.getString("hor_Importacion"));
                notaDespachoDetalle.setCodUsuarioImp(rs.getInt("cod_usuario_imp"));
                notaDespachoDetalle.setFecCreacion(rs.getString("fec_creacion"));
                notaDespachoDetalle.setHorCreacion(rs.getString("hor_creacion"));
                notaDespachoDetalle.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                notaDespachoDetalle.setFecActualizacion(rs.getString("fec_actualizacion"));
                notaDespachoDetalle.setHorActualizacion(rs.getString("hor_actualizacion"));
                notaDespachoDetalle.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
                notaDespachoDetalle.setAimarProductos(pDAO.consultarObjProducto(notaDespachoDetalle.getCodProducto()));

                notaDespachoDetalle.setAgmaeCentrocosto(cCDAO.consultarObjCentroCosto(notaDespachoDetalle.getCodCentroc()));
                notaDespachoDetalle.setAgmaeArea(areaDAO.consultarObjArea(notaDespachoDetalle.getRucCompanyia(), notaDespachoDetalle.getCodArea()));
                notaDespachoDetalle.setAimarAlmacen(almDAO.consultarObjAlmacen(notaDespachoDetalle.getRucCompanyia(), notaDespachoDetalle.getCodAlmacen()));

                //se puede hacer en un solo campo pero para mayor entendimiento lo colocare asi
                notaDespachoDetalle.setNuevo(false);
                notaDespachoDetalle.setEditado(false);

                lista.add(notaDespachoDetalle);
            }
            if (!existe) {
                lista = null;
                System.out.println("No se encontro datos");
            }
//  Service.cerrarConexion();
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
    public List<AvmovMovNotaDespachoDet> listarNotaDespachoDetalleSinGR(AvmovMovNotaDespachoCab notaDespacho, List<AvmovGuiaRemisionDet> guiaRemisionDetalle) {
        String idFacturaNoVa = "0";
        String idNotaDespachoNoVa = "0";
        if (guiaRemisionDetalle.size() > 0 && guiaRemisionDetalle != null) {
            for (AvmovGuiaRemisionDet item : guiaRemisionDetalle) {
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
        AvmovMovNotaDespachoDet notaDespachoDetalle = null;
        List<AvmovMovNotaDespachoDet> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_MovNotaDespachoDet.id_MovValeProducto, "
                    + "AVMOV_MovNotaDespachoDet.id_MovValeCab, "
                    + "AVMOV_MovNotaDespachoDet.ruc_Companyia, "
                    + "AVMOV_MovNotaDespachoDet.cod_producto, "
                    + "AIMAR_Productos.Des_Ai_Produc, "
                    + "AVMOV_MovNotaDespachoDet.ctd_Movimiento, "
                    + "AIMAR_Unidad_Medida.CodMed_AI_UniMed, "
                    + "AVMOV_MovNotaDespachoDet.flg_estado_guia_remision, "
                    + "AVMOV_MovNotaDespachoDet.flg_estado_factura "
                    + "FROM AIMAR_Unidad_Medida RIGHT JOIN (AIMAR_Productos RIGHT JOIN AVMOV_MovNotaDespachoDet ON (AIMAR_Productos.Cod_Ai_Produc = AVMOV_MovNotaDespachoDet.cod_producto) AND (AIMAR_Productos.ruc_companyia = AVMOV_MovNotaDespachoDet.ruc_Companyia)) ON (AIMAR_Unidad_Medida.CodMed_AI_UniMed = AIMAR_Productos.Cod_Medida) AND (AIMAR_Unidad_Medida.ruc_companyia = AIMAR_Productos.ruc_companyia) "
                    + "WHERE (((AVMOV_MovNotaDespachoDet.flg_estado_guia_remision)<>1) "
                    + "AND (AVMOV_MovNotaDespachoDet.id_MovValeProducto NOT IN(" + idNotaDespachoNoVa + ")) "
                    + "AND (AVMOV_MovNotaDespachoDet.id_MovValeProducto NOT IN(SELECT AVMOV_Factura_ND_DET.idNotaDespachoDet FROM AVMOV_Factura_ND_DET WHERE (((AVMOV_Factura_ND_DET.idFacturaDet) In (" + idFacturaNoVa + "))))) "
                    + "AND (AVMOV_MovNotaDespachoDet.id_MovValeCab=?));";
            ps = cn.prepareStatement(sql);
            ps.setInt(1, notaDespacho.getIdMovValeCab());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                notaDespachoDetalle = new AvmovMovNotaDespachoDet();
                notaDespachoDetalle.setIdMovValeProducto(rs.getInt(1));
                notaDespachoDetalle.setIdMovValeCab(rs.getInt(2));
                notaDespachoDetalle.setRucCompanyia(rs.getString(3));
                notaDespachoDetalle.setCodProducto(rs.getString(4));
                notaDespachoDetalle.setNomProducto(rs.getString(5));
                notaDespachoDetalle.setCtdMovimiento(rs.getInt(6));
                notaDespachoDetalle.setCodMedida(rs.getString(7));
                notaDespachoDetalle.setFlgEstadoGuiaRemision(rs.getInt(8));
                notaDespachoDetalle.setFlgEstadoFactura(rs.getInt(9));

                lista.add(notaDespachoDetalle);

            }

            if (!existe) {
                notaDespachoDetalle = null;
                System.out.println("No se encontro datos");
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

        return lista;
    }

    @Override
    public AvmovMovNotaDespachoDet obtenerIdNotaDespachoDetalle(AvmovMovNotaDespachoDet nDD) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovMovNotaDespachoDet notaDespachoDetalle = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT id_MovValeProducto "
                    + "FROM AVMOV_MovNotaDespachoDet "
                    + "WHERE num_vale=? AND cod_producto=? AND ctd_Movimiento=? "
                    + "ORDER BY id_MovValeProducto DESC; ";

            ps = cn.prepareStatement(sql);
            ps.setString(1, nDD.getNumVale());
            ps.setString(2, nDD.getCodProducto());
            ps.setInt(3, nDD.getCtdMovimiento());
            rs = ps.executeQuery();
            if (rs.next()) {
                existe = true;
                notaDespachoDetalle = new AvmovMovNotaDespachoDet();

                notaDespachoDetalle.setIdMovValeProducto(rs.getInt("id_MovValeProducto"));

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
        return notaDespachoDetalle;

    }

    @Override
    public void newNotaDespachoDetalle(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AVMOV_MovNotaDespachoDet (`id_MovValeCab`, "
                + "`ruc_Companyia`, `num_vale`, `cod_establecimiento`, `cod_centroc`, "
                + "`cod_area`, `cod_almacen`, `cod_campo`, `cod_sector`, "
                + "`cod_producto`, `ctd_dosis`, `num_tancadas`, `num_has`, "
                + "`ctd_total`, `ctd_Movimiento`, `pre_vta`, `imp_vta`, `val_vta_sol`, "
                + "`imp_vta_sol`, `val_vta_dol`, `imp_vta_dol`, `Exportado`, "
                + "`Importado`, `fec_Importacion`, `hor_Importacion`, `cod_usuario_imp`, "
                + "`fec_creacion`, `hor_creacion`, `cod_usuario_creacion`, `fec_actualizacion`, "
                + "`hor_actualizacion`, `cod_usuario_actualizacion`,`cod_presentacion`,`num_cantidad_presentacion`,`flg_estado_factura`) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespachoDetalle.setFecCreacion(formatoFecha.format(fechaActual));
        notaDespachoDetalle.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);

            ps.setInt(1, notaDespachoDetalle.getIdMovValeCab());
            ps.setString(2, notaDespachoDetalle.getRucCompanyia());
            ps.setString(3, notaDespachoDetalle.getNumVale());
            ps.setString(4, notaDespachoDetalle.getCodEstablecimiento());
            ps.setInt(5, notaDespachoDetalle.getCodCentroc());
            ps.setInt(6, notaDespachoDetalle.getCodArea());
            ps.setInt(7, notaDespachoDetalle.getCodAlmacen());
            ps.setInt(8, notaDespachoDetalle.getCodCampo());
            ps.setInt(9, notaDespachoDetalle.getCodSector());
            ps.setString(10, notaDespachoDetalle.getCodProducto());
            ps.setString(11, notaDespachoDetalle.getCtdDosis());
            ps.setString(12, notaDespachoDetalle.getNumTancadas());
            ps.setInt(13, notaDespachoDetalle.getNumHas());
            ps.setInt(14, notaDespachoDetalle.getCtdTotal());
            ps.setInt(15, notaDespachoDetalle.getCtdMovimiento());
            ps.setInt(16, notaDespachoDetalle.getPreVta());
            ps.setInt(17, notaDespachoDetalle.getImpVta());
            ps.setInt(18, notaDespachoDetalle.getValVtaSol());
            ps.setInt(19, notaDespachoDetalle.getImpVtaSol());
            ps.setInt(20, notaDespachoDetalle.getValVtaDol());
            ps.setInt(21, notaDespachoDetalle.getImpVtaDol());
            ps.setString(22, notaDespachoDetalle.getExportado());
            ps.setString(23, notaDespachoDetalle.getImportado());
            ps.setString(24, notaDespachoDetalle.getFecImportacion());
            ps.setString(25, notaDespachoDetalle.getHorImportacion());
            ps.setInt(26, notaDespachoDetalle.getCodUsuarioImp());
            ps.setString(27, notaDespachoDetalle.getFecCreacion());
            ps.setString(28, notaDespachoDetalle.getHorCreacion());
            ps.setInt(29, notaDespachoDetalle.getCodUsuarioCreacion());
            ps.setString(30, notaDespachoDetalle.getFecActualizacion());
            ps.setString(31, notaDespachoDetalle.getHorActualizacion());
            ps.setInt(32, notaDespachoDetalle.getCodUsuarioActualizacion());
            ps.setInt(33, notaDespachoDetalle.getCodPresentacion());
            ps.setInt(34, notaDespachoDetalle.getNumCantidadPresentacion());
            ps.setInt(35, notaDespachoDetalle.getFlgEstadoFactura());

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
    public void updateNotaDespachoDetalle(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AVMOV_MovNotaDespachoDet SET id_MovValeCab=?, "
                + "ruc_Companyia=?, num_vale=?, cod_establecimiento=?, cod_centroc=?, "
                + "cod_area=?, cod_almacen=?, cod_campo=?, cod_sector=?, "
                + "cod_producto=?, ctd_dosis=?, num_tancadas=?, num_has=?, ctd_total=?, "
                + "ctd_Movimiento=?, pre_vta=?, imp_vta=?, val_vta_sol=?, imp_vta_sol=?, "
                + "val_vta_dol=?, imp_vta_dol=?, Exportado=?, Importado=?, "
                + "fec_Importacion=?, hor_Importacion=?, cod_usuario_imp=?, "
                + "fec_creacion=?, hor_creacion=?, cod_usuario_creacion=?, "
                + "fec_actualizacion=?, hor_actualizacion=?, cod_usuario_actualizacion=? , cod_presentacion=?, "
                + "num_cantidad_presentacion=?, flg_estado_factura=? "
                + "WHERE id_MovValeProducto=?;";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespachoDetalle.setFecActualizacion(formatoFecha.format(fechaActual));
        notaDespachoDetalle.setHorActualizacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);

            ps.setInt(1, notaDespachoDetalle.getIdMovValeCab());
            ps.setString(2, notaDespachoDetalle.getRucCompanyia());
            ps.setString(3, notaDespachoDetalle.getNumVale());
            ps.setString(4, notaDespachoDetalle.getCodEstablecimiento());
            ps.setInt(5, notaDespachoDetalle.getCodCentroc());
            ps.setInt(6, notaDespachoDetalle.getCodArea());
            ps.setInt(7, notaDespachoDetalle.getCodAlmacen());
            ps.setInt(8, notaDespachoDetalle.getCodCampo());
            ps.setInt(9, notaDespachoDetalle.getCodSector());
            ps.setString(10, notaDespachoDetalle.getCodProducto());
            ps.setString(11, notaDespachoDetalle.getCtdDosis());
            ps.setString(12, notaDespachoDetalle.getNumTancadas());
            ps.setInt(13, notaDespachoDetalle.getNumHas());
            ps.setInt(14, notaDespachoDetalle.getCtdTotal());
            ps.setInt(15, notaDespachoDetalle.getCtdMovimiento());
            ps.setInt(16, notaDespachoDetalle.getPreVta());
            ps.setInt(17, notaDespachoDetalle.getImpVta());
            ps.setInt(18, notaDespachoDetalle.getValVtaSol());
            ps.setInt(19, notaDespachoDetalle.getImpVtaSol());
            ps.setInt(20, notaDespachoDetalle.getValVtaDol());
            ps.setInt(21, notaDespachoDetalle.getImpVtaDol());
            ps.setString(22, notaDespachoDetalle.getExportado());
            ps.setString(23, notaDespachoDetalle.getImportado());
            ps.setString(24, notaDespachoDetalle.getFecImportacion());
            ps.setString(25, notaDespachoDetalle.getHorImportacion());
            ps.setInt(26, notaDespachoDetalle.getCodUsuarioImp());
            ps.setString(27, notaDespachoDetalle.getFecCreacion());
            ps.setString(28, notaDespachoDetalle.getHorCreacion());
            ps.setInt(29, notaDespachoDetalle.getCodUsuarioCreacion());
            ps.setString(30, notaDespachoDetalle.getFecActualizacion());
            ps.setString(31, notaDespachoDetalle.getHorActualizacion());
            ps.setInt(32, notaDespachoDetalle.getCodUsuarioActualizacion());
            ps.setInt(33, notaDespachoDetalle.getCodPresentacion());
            ps.setInt(34, notaDespachoDetalle.getNumCantidadPresentacion());
            ps.setInt(35, notaDespachoDetalle.getFlgEstadoFactura());

            ps.setInt(36, notaDespachoDetalle.getIdMovValeProducto());

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
    public void updateFactutacionNotaDespachoDetalle(int idNotaDespachoDet, int flgEstado) {
        Connection cn = null;
        PreparedStatement ps = null;
        AvmovMovNotaDespachoDet notaDespachoDetalle = new AvmovMovNotaDespachoDet();
        String sql = "UPDATE AVMOV_MovNotaDespachoDet SET "
                + "flg_estado_factura=?, "
                + "fec_actualizacion=?, "
                + "hor_actualizacion=?, "
                + "cod_usuario_actualizacion=? "
                + "WHERE id_MovValeProducto=?;";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespachoDetalle.setFecActualizacion(formatoFecha.format(fechaActual));
        notaDespachoDetalle.setHorActualizacion(formatoHora.format(fechaActual));
        notaDespachoDetalle.setIdMovValeProducto(idNotaDespachoDet);
        notaDespachoDetalle.setFlgEstadoFactura(flgEstado);

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);

            ps.setInt(1, notaDespachoDetalle.getFlgEstadoFactura());
            ps.setString(2, notaDespachoDetalle.getFecActualizacion());
            ps.setString(3, notaDespachoDetalle.getHorActualizacion());
            ps.setInt(4, notaDespachoDetalle.getCodUsuarioActualizacion());
            ps.setInt(5, notaDespachoDetalle.getIdMovValeProducto());

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
    public void deleteNotaDespachoDetalle(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AVMOV_MovNotaDespachoDet WHERE id_MovValeProducto=?;";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, notaDespachoDetalle.getIdMovValeProducto());
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
    public List<AvmovMovNotaDespachoDet> listarNotaDespachoDetalles(AvmovMovNotaDespachoCab nd) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovMovNotaDespachoDet notaDespachoDetalle = null;
        List<AvmovMovNotaDespachoDet> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_MovNotaDespachoDet.id_MovValeProducto AS codMovDet, \n"
                    + "AVMOV_MovNotaDespachoDet.id_MovValeCab AS codMovCab, \n"
                    + "AVMOV_MovNotaDespachoDet.num_vale AS numVale, \n"
                    + "AVMOV_MovNotaDespachoDet.ruc_Companyia AS ruc, \n"
                    + "agmae_companyias.des_companyia AS nomCompanya, \n"
                    + "AVMOV_MovNotaDespachoDet.cod_producto AS codProducto, \n"
                    + "AIMAR_Productos.Des_Ai_Produc  AS nomProducto, \n"
                    + "AVMOV_MovNotaDespachoDet.cod_presentacion AS codPresentacion, \n"
                    + "AIMAR_Presentaciones.des_presentacion AS nomPresentacion, \n"
                    + "AIMAR_Productos_Presentacion.val_equivalencia AS numPresentacionEquivalencia, \n"
                    + "AIMAR_Productos.Cod_Medida AS codMedida, \n"
                    + "AIMAR_Unidad_Medida.NomMed_AI_UniMed  AS nomMedida, \n"
                    + "AVMOV_MovNotaDespachoDet.ctd_Movimiento AS ctdMov, \n"
                    + "stock.stockActual AS sActual, \n"
                    + "stock.stockFecha AS sFecha, \n"
                    + "AVMOV_MovNotaDespachoDet.cod_establecimiento  AS codEstablecimiento, \n"
                    + "AVMOV_MovNotaDespachoDet.cod_area AS codArea, \n"
                    + "AVMOV_MovNotaDespachoDet.cod_almacen  AS codAlmacen, \n"
                    + "AVMOV_MovNotaDespachoDet.cod_centroc AS codCentro, \n"
                    + "AVMOV_MovNotaDespachoDet.num_cantidad_presentacion AS numCantidadPresentacion \n"
                    + "FROM (AIMAR_Unidad_Medida RIGHT JOIN (AIMAR_Productos_Presentacion RIGHT JOIN ((AIMAR_Productos "
                    + "RIGHT JOIN (AVMOV_MovNotaDespachoDet LEFT JOIN agmae_companyias ON AVMOV_MovNotaDespachoDet.ruc_Companyia = "
                    + "agmae_companyias.ruc_companyia) ON (AIMAR_Productos.ruc_companyia = AVMOV_MovNotaDespachoDet.ruc_Companyia) "
                    + "AND (AIMAR_Productos.Cod_Ai_Produc = AVMOV_MovNotaDespachoDet.cod_producto)) LEFT JOIN AIMAR_Presentaciones "
                    + "ON AVMOV_MovNotaDespachoDet.cod_presentacion = AIMAR_Presentaciones.cod_presentacion) ON "
                    + "(AIMAR_Productos_Presentacion.cod_presentacion = AVMOV_MovNotaDespachoDet.cod_presentacion) "
                    + "AND (AIMAR_Productos_Presentacion.cod_producto = AVMOV_MovNotaDespachoDet.cod_producto)) "
                    + "ON (AIMAR_Unidad_Medida.CodMed_AI_UniMed = AIMAR_Productos.Cod_Medida) AND (AIMAR_Unidad_Medida.ruc_companyia = "
                    + "AIMAR_Productos.ruc_companyia)) LEFT JOIN (SELECT Sum(IIf([AIMAR_MovAlmacenCab].[Cod_TipoConcepto]='I',"
                    + "[MovDet].[num_cantidad],(-1)*[MovDet].[num_cantidad])) AS stockActual, Sum(IIf([AIMAR_MovAlmacenCab].[fec_movimiento]<=?,IIf([AIMAR_MovAlmacenCab].[Cod_TipoConcepto]='I',[MovDet].[num_cantidad],(-1)*[MovDet].[num_cantidad]),0)) AS stockFecha, MovDet.ruc_companyia AS codCompanya, MovDet.cod_establecimiento AS codEstablecimiento, MovDet.cod_centroc AS codCentro, "
                    + "MovDet.Cod_Area AS codArea, MovDet.cod_almacen AS codAlmacen, MovDet.cod_producto AS codProducto\n"
                    + "FROM (SELECT * FROM AIMAR_MovAlmacenDet UNION ALL SELECT * FROM AIMAR_MovAlmacenDet_Temp) "
                    + " AS MovDet INNER JOIN AIMAR_MovAlmacenCab ON MovDet.IdMovAlmCab = AIMAR_MovAlmacenCab.IdMovAlmCab\n"
                    + "GROUP BY MovDet.ruc_companyia, MovDet.cod_establecimiento, MovDet.cod_centroc, MovDet.Cod_Area, MovDet.cod_almacen, MovDet.cod_producto\n"
                    + ")  AS stock ON (AVMOV_MovNotaDespachoDet.cod_establecimiento = stock.codEstablecimiento) AND (AVMOV_MovNotaDespachoDet.cod_almacen = stock.codAlmacen) AND (AVMOV_MovNotaDespachoDet.cod_area = stock.codArea) AND (AVMOV_MovNotaDespachoDet.cod_producto = stock.codProducto) AND (AVMOV_MovNotaDespachoDet.ruc_Companyia = stock.codCompanya) AND (AVMOV_MovNotaDespachoDet.cod_centroc = stock.codCentro)"
                    + "WHERE (((AVMOV_MovNotaDespachoDet.num_vale)=?));";

            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(nd.getFecVale()));
            ps.setString(2, nd.getNumVale());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                notaDespachoDetalle = new AvmovMovNotaDespachoDet();

                notaDespachoDetalle.setIdMovValeProducto(rs.getInt("codMovDet"));
                notaDespachoDetalle.setIdMovValeCab(rs.getInt("codMovCab"));
                notaDespachoDetalle.setRucCompanyia(rs.getString("ruc"));
                notaDespachoDetalle.setNumVale(rs.getString("numVale"));
                notaDespachoDetalle.setCodEstablecimiento(rs.getString("codEstablecimiento"));
                notaDespachoDetalle.setCodCentroc(rs.getInt("codCentro"));
                notaDespachoDetalle.setCodArea(rs.getInt("codArea"));
                notaDespachoDetalle.setCodAlmacen(rs.getInt("codAlmacen"));
                notaDespachoDetalle.setCodProducto(rs.getString("codProducto"));
                notaDespachoDetalle.setCodPresentacion(rs.getInt("codPresentacion"));

                notaDespachoDetalle.setNomCompanya(rs.getString("nomCompanya"));
                notaDespachoDetalle.setNomProducto(rs.getString("nomProducto"));
                notaDespachoDetalle.setNomPresentacion(rs.getString("nomPresentacion"));
                notaDespachoDetalle.setNumPresentacionEquivalencia(rs.getInt("numPresentacionEquivalencia"));
                notaDespachoDetalle.setCodMedida(rs.getString("codMedida"));
                notaDespachoDetalle.setNomMedida(rs.getString("nomMedida"));
                notaDespachoDetalle.setStockActual(rs.getInt("sActual"));
                notaDespachoDetalle.setStockFecha(rs.getInt("sFecha"));

                notaDespachoDetalle.setCtdMovimiento(rs.getInt("ctdMov"));
                notaDespachoDetalle.setNumCantidadPresentacion(rs.getInt("numCantidadPresentacion"));
                
                lista.add(notaDespachoDetalle);
            }
            if (!existe) {
                lista = null;
                System.out.println("No se encontro datos");
            }
//  Service.cerrarConexion();
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

}
