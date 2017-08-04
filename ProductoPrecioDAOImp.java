package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import sys.dao.ProductoDAO;
import sys.dao.ProductoPrecioDAO;
import sys.model.ZProductoPrecio;

import sys.util.Service;

public class ProductoPrecioDAOImp implements ProductoPrecioDAO {

    @Override
    public List<ZProductoPrecio> listarProductoPrecios() {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        ZProductoPrecio productoPrecio = null;
        List<ZProductoPrecio> lista = new ArrayList<>();
                ProductoDAO pDAO = new ProductoDAOImp();
        try {
            cn = Service.getConexion();
            String sql = "SELECT Cod_Producto,Precio_Producto,fec_creacion, "
                    + "hor_creacion, cod_usuario_creacion, fec_actualizacion, "
                    + "hor_actualizacion, cod_usuario_actualizacion "
                    + "FROM Z_Producto_Precio";
            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                productoPrecio = new ZProductoPrecio();
                productoPrecio.setCodProducto(rs.getString("Cod_Producto"));
                productoPrecio.setPrecioProducto(rs.getDouble("Precio_Producto"));
                productoPrecio.setAimarProductos(pDAO.consultarObjProducto(productoPrecio.getCodProducto()));
                productoPrecio.setFecCreacion(rs.getString("fec_creacion"));
                productoPrecio.setHorCreacion(rs.getString("hor_creacion"));
                productoPrecio.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                productoPrecio.setFecActualizacion(rs.getString("fec_actualizacion"));
                productoPrecio.setHorActualizacion(rs.getString("hor_actualizacion"));
                productoPrecio.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));

                lista.add(productoPrecio);
            }
            if (!existe) {
                productoPrecio = null;
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
    public void newProductoPrecio(ZProductoPrecio productoPrecio) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO Z_Producto_Precio (`Cod_Producto`,`Precio_Producto`,`fec_creacion`,`hor_creacion`,`cod_usuario_creacion`) VALUES(?,?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        productoPrecio.setFecCreacion(formatoFecha.format(fechaActual));
        productoPrecio.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, productoPrecio.getCodProducto());
            ps.setDouble(2, productoPrecio.getPrecioProducto());
            ps.setString(3, productoPrecio.getFecCreacion());
            ps.setString(4, productoPrecio.getHorCreacion());
            ps.setInt(5, productoPrecio.getCodUsuarioCreacion());

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
    public void updateProductoPrecio(ZProductoPrecio productoPrecio) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE Z_Producto_Precio SET Precio_Producto=?, fec_actualizacion=?, hor_actualizacion=?, cod_usuario_actualizacion=? WHERE Cod_Producto=?;";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        productoPrecio.setFecActualizacion(formatoFecha.format(fechaActual));
        productoPrecio.setHorActualizacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setDouble(1, productoPrecio.getPrecioProducto());
            ps.setString(2, productoPrecio.getFecActualizacion());
            ps.setString(3, productoPrecio.getHorActualizacion());
            ps.setInt(4, productoPrecio.getCodUsuarioActualizacion());

            ps.setString(5, productoPrecio.getCodProducto());

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
    public void deleteProductoPrecio(ZProductoPrecio productoPrecio) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM Z_Producto_Precio WHERE Cod_Producto=?;";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, productoPrecio.getCodProducto());
            ps.executeUpdate();
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
    }

}
