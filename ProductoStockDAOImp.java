package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sys.dao.AlmacenDAO;
import sys.dao.AreaDAO;
import sys.dao.CentroCostoDAO;
import sys.dao.ProductoDAO;
import sys.model.AimarStockActual;
import sys.util.Service;
import sys.dao.ProductoStockDAO;
import sys.model.ZProductoPrecio;

public class ProductoStockDAOImp implements ProductoStockDAO {

    @Override
    public List<AimarStockActual> listarProductoStocks() {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarStockActual productoStock = null;
        List<AimarStockActual> lista = new ArrayList<>();
        ProductoDAO pDAO = new ProductoDAOImp();
        
        CentroCostoDAO cCDAO = new CentroCostoDAOImp();
        AreaDAO areaDAO = new AreaDAOImp();
        AlmacenDAO almDAO=new AlmacenDAOImp();
        
        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, COD_establecimiento, cod_centroc, "
                    + "Cod_Ai_Almacen, Cod_Area, Cod_Ai_Produc, cod_presentacion, "
                    + "num_cantidad_presentacion, stock_inicial, stock_log, "
                    + "stock_actual, fec_ult_inv, costo_prom_sol, costo_prom_dol, "
                    + "costo_prom_ini_sol, costo_prom_ini_dol, total_ing_sol, "
                    + "total_ing_dol, total_sal_sol, total_sal_dol, total_cant_ingreso, "
                    + "total_cant_salida, total_stock_ini_sol, total_stock_ini_dol, "
                    + "fec_creacion, hor_creacion, cod_usuario_creacion, "
                    + "fec_actualizacion, hor_actualizacion, cod_usuario_actualizacion "
                    + "FROM AIMAR_StockActual; ";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                productoStock = new AimarStockActual();

                productoStock.setRucCompanyia(rs.getString("ruc_companyia"));
                productoStock.setCodEstablecimiento(rs.getString("COD_establecimiento"));
                productoStock.setCodCentroc(rs.getInt("cod_centroc"));
                productoStock.setCodAiAlmacen(rs.getInt("Cod_Ai_Almacen"));
                productoStock.setCodArea(rs.getInt("Cod_Area"));
                productoStock.setCodAiProduc(rs.getString("Cod_Ai_Produc"));
                productoStock.setCodPresentacion(rs.getInt("cod_presentacion"));
                productoStock.setNumCantidadPresentacion(rs.getInt("num_cantidad_presentacion"));
                productoStock.setStockInicial(rs.getInt("stock_inicial"));
                productoStock.setStockLog(rs.getInt("stock_log"));
                productoStock.setStockActual(rs.getInt("stock_actual"));
                productoStock.setFecUltInv(rs.getString("fec_ult_inv"));
                productoStock.setCostoPromSol(rs.getInt("costo_prom_sol"));
                productoStock.setCostoPromDol(rs.getInt("costo_prom_dol"));
                productoStock.setCostoPromIniSol(rs.getInt("costo_prom_ini_sol"));
                productoStock.setCostoPromIniDol(rs.getInt("costo_prom_ini_dol"));
                productoStock.setTotalIngSol(rs.getInt("total_ing_sol"));
                productoStock.setTotalIngDol(rs.getInt("total_ing_dol"));
                productoStock.setTotalSalSol(rs.getInt("total_sal_sol"));
                productoStock.setTotalSalDol(rs.getInt("total_sal_dol"));
                productoStock.setTotalCantIngreso(rs.getInt("total_cant_ingreso"));
                productoStock.setTotalCantSalida(rs.getInt("total_cant_salida"));
                productoStock.setTotalStockIniSol(rs.getInt("total_stock_ini_sol"));
                productoStock.setTotalStockIniDol(rs.getInt("total_stock_ini_dol"));
                productoStock.setFecCreacion(rs.getString("fec_creacion"));
                productoStock.setHorCreacion(rs.getString("hor_creacion"));
                productoStock.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                productoStock.setFecActualizacion(rs.getString("fec_actualizacion"));
                productoStock.setHorActualizacion(rs.getString("hor_actualizacion"));
                productoStock.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
                productoStock.setAimarProductos(pDAO.consultarObjProducto(productoStock.getCodAiProduc()));

                productoStock.setAgmaeCentrocosto(cCDAO.consultarObjCentroCosto(productoStock.getCodCentroc()));
                productoStock.setAgmaeArea(areaDAO.consultarObjArea(productoStock.getRucCompanyia() ,productoStock.getCodArea()));
                productoStock.setAimarAlmacen(almDAO.consultarObjAlmacen(productoStock.getRucCompanyia() ,productoStock.getCodAiAlmacen()));
                
                
                
                lista.add(productoStock);
            }
            if (!existe) {
                productoStock = null;
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
    public List<AimarStockActual> obtenerProductoStocks(ZProductoPrecio productoPrecio) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarStockActual productoStock = null;
        List<AimarStockActual> lista = new ArrayList<>();
        ProductoDAO pDAO = new ProductoDAOImp();

        try {
            cn = Service.getConexion();
            String sql = "SELECT ruc_companyia, COD_establecimiento, cod_centroc, "
                    + "Cod_Ai_Almacen, Cod_Area, Cod_Ai_Produc, cod_presentacion, "
                    + "num_cantidad_presentacion, stock_inicial, stock_log, "
                    + "stock_actual, fec_ult_inv, costo_prom_sol, costo_prom_dol, "
                    + "costo_prom_ini_sol, costo_prom_ini_dol, total_ing_sol, "
                    + "total_ing_dol, total_sal_sol, total_sal_dol, total_cant_ingreso, "
                    + "total_cant_salida, total_stock_ini_sol, total_stock_ini_dol, "
                    + "fec_creacion, hor_creacion, cod_usuario_creacion, "
                    + "fec_actualizacion, hor_actualizacion, cod_usuario_actualizacion "
                    + "FROM AIMAR_StockActual "
                    + "WHERE Cod_Ai_Produc=?; ";

            ps = cn.prepareStatement(sql);
            ps.setString(1, productoPrecio.getCodProducto());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                productoStock = new AimarStockActual();

                productoStock.setRucCompanyia(rs.getString("ruc_companyia"));
                productoStock.setCodEstablecimiento(rs.getString("COD_establecimiento"));
                productoStock.setCodCentroc(rs.getInt("cod_centroc"));
                productoStock.setCodAiAlmacen(rs.getInt("Cod_Ai_Almacen"));
                productoStock.setCodArea(rs.getInt("Cod_Area"));
                productoStock.setCodAiProduc(rs.getString("Cod_Ai_Produc"));
                productoStock.setCodPresentacion(rs.getInt("cod_presentacion"));
                productoStock.setNumCantidadPresentacion(rs.getInt("num_cantidad_presentacion"));
                productoStock.setStockInicial(rs.getInt("stock_inicial"));
                productoStock.setStockLog(rs.getInt("stock_log"));
                productoStock.setStockActual(rs.getInt("stock_actual"));
                productoStock.setFecUltInv(rs.getString("fec_ult_inv"));
                productoStock.setCostoPromSol(rs.getInt("costo_prom_sol"));
                productoStock.setCostoPromDol(rs.getInt("costo_prom_dol"));
                productoStock.setCostoPromIniSol(rs.getInt("costo_prom_ini_sol"));
                productoStock.setCostoPromIniDol(rs.getInt("costo_prom_ini_dol"));
                productoStock.setTotalIngSol(rs.getInt("total_ing_sol"));
                productoStock.setTotalIngDol(rs.getInt("total_ing_dol"));
                productoStock.setTotalSalSol(rs.getInt("total_sal_sol"));
                productoStock.setTotalSalDol(rs.getInt("total_sal_dol"));
                productoStock.setTotalCantIngreso(rs.getInt("total_cant_ingreso"));
                productoStock.setTotalCantSalida(rs.getInt("total_cant_salida"));
                productoStock.setTotalStockIniSol(rs.getInt("total_stock_ini_sol"));
                productoStock.setTotalStockIniDol(rs.getInt("total_stock_ini_dol"));
                productoStock.setFecCreacion(rs.getString("fec_creacion"));
                productoStock.setHorCreacion(rs.getString("hor_creacion"));
                productoStock.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                productoStock.setFecActualizacion(rs.getString("fec_actualizacion"));
                productoStock.setHorActualizacion(rs.getString("hor_actualizacion"));
                productoStock.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
                productoStock.setAimarProductos(pDAO.consultarObjProducto(productoStock.getCodAiProduc()));

                lista.add(productoStock);
            }
            if (!existe) {
                productoStock = null;
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

    
    
}
